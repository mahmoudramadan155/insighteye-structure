# app/services/stream_service.py
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple, Union
from uuid import UUID
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import HTTPException, WebSocket
from starlette.websockets import WebSocketState

from app.services.database import db_manager
from app.services.fire_detection_service import fire_detection_service
from app.services.people_count_service import people_count_service
from app.services.notification_service import notification_service
from app.services.video_stream_service import video_stream_service
from app.services.parameter_service import parameter_service
# from app.services.qdrant_service import qdrant_service  
from app.services.unified_data_service import unified_data_service as qdrant_service
from app.services.workspace_service import workspace_service
from app.services.shared_stream_service import video_file_manager
from app.services.stream_processing_service import stream_processing_service
from app.config.settings import config
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)


class StreamState(Enum):
    """Stream lifecycle states."""
    STARTING = "starting"
    ACTIVE = "active"
    STOPPING = "stopping"
    ERROR = "error"
    INACTIVE = "inactive"


class WorkspaceQuotaExceeded(Exception):
    """Raised when workspace stream quota is exceeded."""
    pass


class StreamManager:
    """
    Centralized stream manager with deep workspace integration.
    with better error handling, state management, and monitoring.
    """
    def __init__(self):
        # Core dependencies
        self.db_manager = db_manager
        self.video_file_manager = video_file_manager
        self.qdrant_service = qdrant_service
        
        # Service dependencies
        self.fire_service = fire_detection_service
        self.people_service = people_count_service
        self.notification_service = notification_service
        self.video_stream_service = video_stream_service
        self.parameter_service = parameter_service
        self.workspace_service = workspace_service
        self.processing_service = stream_processing_service
        
        # locking with timeout support
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        
        # Stream state management with tracking
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_states: Dict[str, StreamState] = {}  # NEW: Explicit state tracking
        self.stream_processing_stats: Dict[str, Dict[str, Any]] = {}
        self.stream_errors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)  # NEW: Error tracking
        
        # Workspace-aware stream registry
        self.workspace_streams: Dict[str, Set[str]] = defaultdict(set)
        self.stream_workspaces: Dict[str, str] = {}
        
        # Notification management
        self.notification_subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        
        # Cooldown tracking with expiration
        self.people_count_notification_cooldowns: Dict[str, float] = {}
        self.people_count_cooldown_duration = config.get("people_count_cooldown_seconds", 300.0)
        
        # Fire detection tracking
        self.fire_detection_states: Dict[str, str] = {}
        self.fire_detection_frame_counts: Dict[str, int] = {}
        self.fire_cooldown_duration = config.get("fire_cooldown_seconds", 300.0)
        
        # Health check configuration
        self.last_healthcheck = datetime.now(timezone.utc)
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        
        # Background tasks with monitoring
        self.background_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        self.monitor_task: Optional[asyncio.Task] = None  # NEW: Resource monitoring
        
        # Shared stream management (FIXED: proper cleanup)
        self._shared_stream_registry: Dict[str, Set[str]] = defaultdict(set)  # source -> stream_ids
        
        # Performance metrics (NEW)
        self.metrics = {
            'total_streams_started': 0,
            'total_streams_stopped': 0,
            'total_errors': 0,
            'avg_stream_duration': 0.0,
            'last_reset': datetime.now(timezone.utc)
        }
        
        # Resource limits (NEW)
        self.max_concurrent_streams = config.get("max_concurrent_streams", 100)
        self.max_streams_per_workspace = config.get("max_streams_per_workspace", 50)
        
        # Initialize processing service
        self.processing_service.initialize(
            stream_manager=self,
            video_file_manager=self.video_file_manager,
            qdrant_service=self.qdrant_service
        )
        
        logging.info("StreamManager initialized with workspace integration")

    # ==================== Context Manager for Safe Operations ====================

    @asynccontextmanager
    async def _safe_stream_operation(self, stream_id_str: str, operation: str):
        """
        Context manager for safe stream operations with proper cleanup.
        NEW: Ensures consistent state even on errors.
        """
        try:
            yield
        except Exception as e:
            logger.error(f"Error during {operation} for stream {stream_id_str}: {e}", exc_info=True)
            await self._record_stream_error(stream_id_str, operation, str(e))
            
            # Update state to error
            async with self._state_lock:
                self.stream_states[stream_id_str] = StreamState.ERROR
            
            raise
        finally:
            # Ensure cleanup happens
            pass

    async def _record_stream_error(
        self, 
        stream_id_str: str, 
        operation: str, 
        error_msg: str,
        max_errors: int = 10
    ):
        """
        Record stream error with automatic pruning.
        NEW: Better error tracking and analysis.
        """
        error_record = {
            'timestamp': datetime.now(timezone.utc),
            'operation': operation,
            'error': error_msg
        }
        
        self.stream_errors[stream_id_str].append(error_record)
        
        # Keep only recent errors
        if len(self.stream_errors[stream_id_str]) > max_errors:
            self.stream_errors[stream_id_str] = self.stream_errors[stream_id_str][-max_errors:]
        
        self.metrics['total_errors'] += 1

    # ==================== State Management ====================
    
    async def _transition_stream_state(
        self, 
        stream_id_str: str, 
        new_state: StreamState,
        force: bool = False
    ) -> bool:
        """
        Safely transition stream state with validation.
        NEW: Prevents invalid state transitions.
        """
        async with self._state_lock:
            current_state = self.stream_states.get(stream_id_str)
            
            # Define valid transitions
            valid_transitions = {
                None: {StreamState.STARTING},
                StreamState.STARTING: {StreamState.ACTIVE, StreamState.ERROR, StreamState.STOPPING},
                StreamState.ACTIVE: {StreamState.STOPPING, StreamState.ERROR},
                StreamState.STOPPING: {StreamState.INACTIVE, StreamState.ERROR},
                StreamState.ERROR: {StreamState.STOPPING, StreamState.INACTIVE},
                StreamState.INACTIVE: {StreamState.STARTING}
            }
            
            if not force and current_state not in valid_transitions:
                logger.warning(f"Unknown current state {current_state} for stream {stream_id_str}")
                return False
            
            if not force and new_state not in valid_transitions.get(current_state, set()):
                logger.warning(
                    f"Invalid state transition for stream {stream_id_str}: "
                    f"{current_state} -> {new_state}"
                )
                return False
            
            self.stream_states[stream_id_str] = new_state
            logger.debug(f"Stream {stream_id_str} state: {current_state} -> {new_state}")
            return True

    # ==================== Workspace Integration ====================

    async def validate_workspace_stream_access(
        self,
        user_id: UUID,
        stream_id: UUID,
        required_role: Optional[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Validate user access to a stream through workspace membership.
        Returns: (stream_info, workspace_membership_info)
        """
        # Get stream info
        stream_info = await self.video_stream_service.get_video_stream_by_id(stream_id)
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        workspace_id = stream_info['workspace_id']
        
        # Check workspace membership and role
        membership_info = await self.workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=workspace_id,
            required_role=required_role
        )
        
        return stream_info, membership_info

    async def get_workspace_stream_limits(self, workspace_id: UUID) -> Dict[str, int]:
        """
        Get stream limits for a workspace with caching.
        FIXED: Now correctly counts active streams from database instead of memory.
        """
        # Fetch from database
        query = """
            SELECT 
                SUM(u.count_of_camera) as total_camera_limit,
                COUNT(DISTINCT u.user_id) as active_members,
                COUNT(DISTINCT CASE WHEN u.is_subscribed = TRUE OR u.role = 'admin' 
                    THEN u.user_id END) as subscribed_members
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.workspace_id = $1 AND u.is_active = TRUE
        """
        result = await self.db_manager.execute_query(query, (workspace_id,), fetch_one=True)
        
        if not result:
            return {
                "total_camera_limit": 0,
                "active_members": 0,
                "subscribed_members": 0,
                "current_active": 0,
                "available_slots": 0
            }
        
        # FIXED: Get current active streams from DATABASE, not memory
        # This ensures accuracy even when streams aren't in memory
        active_count_query = """
            SELECT COUNT(*) as count
            FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE
        """
        active_result = await self.db_manager.execute_query(
            active_count_query, (workspace_id,), fetch_one=True
        )
        active_count = active_result['count'] if active_result else 0
        
        total_limit = result.get('total_camera_limit', 0) or 0
        
        limits_data = {
            "total_camera_limit": total_limit,
            "active_members": result.get('active_members', 0),
            "subscribed_members": result.get('subscribed_members', 0),
            "current_active": active_count,
            "available_slots": max(0, total_limit - active_count)
        }
        
        return limits_data

    async def can_start_stream_in_workspace(
        self,
        workspace_id: UUID,
        user_id: UUID
    ) -> Tuple[bool, str]:
        """
        Check if a stream can be started with validation.
        ENHANCED: Better quota checking and error messages.
        """
        workspace_id_str = str(workspace_id)
        
        # Check global system limit
        total_active = len(self.active_streams)
        if total_active >= self.max_concurrent_streams:
            return False, f"System limit reached ({self.max_concurrent_streams} concurrent streams)"
        
        # Check workspace limit
        workspace_active = len(self.workspace_streams.get(workspace_id_str, set()))
        if workspace_active >= self.max_streams_per_workspace:
            return False, f"Workspace limit reached ({self.max_streams_per_workspace} streams)"
        
        # Get workspace quota limits
        limits = await self.get_workspace_stream_limits(workspace_id)
        
        if limits['available_slots'] <= 0:
            return False, (
                f"Workspace camera quota exhausted "
                f"({limits['current_active']}/{limits['total_camera_limit']}). "
                f"Contact workspace admin to increase quota."
            )
        
        # Check user's subscription
        user_query = """
            SELECT is_active, is_subscribed, role, count_of_camera 
            FROM users WHERE user_id = $1
        """
        user_info = await self.db_manager.execute_query(user_query, (user_id,), fetch_one=True)
        
        if not user_info:
            return False, "User not found"
        
        if not user_info['is_active']:
            return False, "User account is inactive"
        
        if not user_info['is_subscribed'] and user_info['role'] != 'admin':
            return False, "Subscription expired. Please renew to start streams."
        
        # Check user's personal limit within workspace
        user_active_query = """
            SELECT COUNT(*) as count FROM video_stream
            WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE
        """
        user_active = await self.db_manager.execute_query(
            user_active_query, (user_id, workspace_id), fetch_one=True
        )
        
        user_active_count = user_active['count'] if user_active else 0
        user_limit = user_info['count_of_camera']
        
        if user_info['role'] != 'admin' and user_active_count >= user_limit:
            return False, (
                f"Personal camera limit reached ({user_active_count}/{user_limit}). "
                f"Stop other cameras or upgrade subscription."
            )
        
        return True, "OK"
    
    async def get_workspace_active_streams(
        self,
        workspace_id: UUID,
        user_id: Optional[UUID] = None
    ) -> List[Dict[str, Any]]:
        """Get all active streams in a workspace with info."""
        workspace_id_str = str(workspace_id)
        stream_ids = self.workspace_streams.get(workspace_id_str, set())
        
        streams_info = []
        async with self._lock:
            for stream_id_str in stream_ids:
                if stream_id_str in self.active_streams:
                    stream_data = self.active_streams[stream_id_str].copy()
                    
                    # Add processing stats
                    if stream_id_str in self.stream_processing_stats:
                        stream_data['stats'] = self.stream_processing_stats[stream_id_str]
                    
                    # Add state
                    stream_data['state'] = self.stream_states.get(stream_id_str, StreamState.ACTIVE).value
                    
                    # Filter by user if specified
                    if user_id and stream_data.get('user_id') != user_id:
                        continue
                    
                    streams_info.append({
                        'stream_id': stream_id_str,
                        'camera_name': stream_data.get('camera_name'),
                        'status': stream_data.get('status'),
                        'state': stream_data['state'],
                        'owner_username': stream_data.get('username'),
                        'start_time': stream_data.get('start_time'),
                        'last_frame_time': stream_data.get('last_frame_time'),
                        'client_count': len(stream_data.get('clients', set())),
                        'stats': stream_data.get('stats', {})
                    })
        
        return streams_info

    async def get_workspace_stream_analytics(
        self,
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """
        Get comprehensive analytics for workspace streams.
        ENHANCED: Added error rates and performance metrics.
        """
        # Get workspace info
        workspace_info = await self.workspace_service.get_workspace_by_id(workspace_id)
        
        # Get stream limits
        limits = await self.get_workspace_stream_limits(workspace_id)
        
        # Get all streams (active and inactive)
        all_streams = await self.video_stream_service.get_workspace_streams(workspace_id)
        
        # Get active streams from memory
        active_streams = await self.get_workspace_active_streams(workspace_id)
        
        # Calculate statistics
        total_streams = len(all_streams)
        streaming_now = len(active_streams)
        
        # Stream types
        stream_types = defaultdict(int)
        for stream in all_streams:
            stream_types[stream.get('type', 'unknown')] += 1
        
        # Alert status
        alerts_enabled = sum(1 for s in all_streams if s.get('alert_enabled'))
        
        # Location distribution
        locations = defaultdict(int)
        for stream in all_streams:
            location = stream.get('location') or 'Unknown'
            locations[location] += 1
        
        # Processing performance
        total_frames = 0
        total_detections = 0
        total_errors = 0
        
        for stream in active_streams:
            stats = stream.get('stats', {})
            total_frames += stats.get('frames_processed', 0)
            total_detections += stats.get('detection_count', 0)
            total_errors += stats.get('errors', 0)
        
        # Error rates
        workspace_id_str = str(workspace_id)
        workspace_stream_ids = self.workspace_streams.get(workspace_id_str, set())
        error_count = sum(
            len(self.stream_errors.get(sid, []))
            for sid in workspace_stream_ids
        )
        
        return {
            'workspace': {
                'id': str(workspace_id),
                'name': workspace_info['name'],
                'is_active': workspace_info['is_active']
            },
            'limits': limits,
            'streams': {
                'total': total_streams,
                'streaming_now': streaming_now,
                'alerts_enabled': alerts_enabled,
                'types': dict(stream_types),
                'locations': dict(locations)
            },
            'performance': {
                'total_frames_processed': total_frames,
                'total_detections': total_detections,
                'total_errors': total_errors,
                'error_rate': (total_errors / total_frames * 100) if total_frames > 0 else 0,
                'avg_detections_per_stream': total_detections / streaming_now if streaming_now > 0 else 0,
                'recent_errors': error_count
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    # ==================== Stream Lifecycle ====================

    async def start_stream_background(
        self,
        stream_id: UUID,
        owner_id: UUID,
        owner_username: str,
        camera_name: str,
        source: str,
        workspace_id: UUID,
        location_info: Optional[Dict[str, Any]] = None
    ):
        """
        Stream start with error handling and state management.
        ENHANCED: Better validation, state tracking, and error recovery.
        """
        stream_id_str = str(stream_id)
        workspace_id_str = str(workspace_id)
        
        # Validate workspace access and limits
        can_start, reason = await self.can_start_stream_in_workspace(workspace_id, owner_id)
        if not can_start:
            logger.warning(f"Cannot start stream {stream_id_str}: {reason}")
            await self.notification_service.create_notification(
                workspace_id=workspace_id,
                user_id=owner_id,
                status="error",
                message=f"Cannot start camera '{camera_name}': {reason}",
                stream_id=stream_id,
                camera_name=camera_name
            )
            raise WorkspaceQuotaExceeded(reason)
        
        async with self._safe_stream_operation(stream_id_str, "start"):
            async with self._lock:
                # Check if already active
                if stream_id_str in self.active_streams:
                    current_state = self.stream_states.get(stream_id_str)
                    if current_state == StreamState.ACTIVE:
                        logger.info(f"Stream {stream_id_str} already active")
                        return
                    elif current_state == StreamState.STARTING:
                        logger.info(f"Stream {stream_id_str} already starting")
                        return
                
                # Transition to STARTING state
                await self._transition_stream_state(stream_id_str, StreamState.STARTING)
                
                # Register stream in workspace
                self.workspace_streams[workspace_id_str].add(stream_id_str)
                self.stream_workspaces[stream_id_str] = workspace_id_str
                
                # Initialize stream entry
                self.active_streams[stream_id_str] = {
                    'status': 'starting',
                    'task': None,
                    'start_time': datetime.now(timezone.utc),
                    'location_info': location_info or {},
                    'workspace_id': workspace_id,
                    'source': source,
                    'camera_name': camera_name,
                    'username': owner_username,
                    'user_id': owner_id,
                    'clients': set(),
                    'latest_frame': None,
                    'last_frame_time': None
                }

            try:
                # Update database status
                await self.video_stream_service.update_stream_status(
                    stream_id, 'processing', is_streaming=None
                )
                
                # Initialize processing stats
                self.stream_processing_stats[stream_id_str] = {
                    "frames_processed": 0,
                    "detection_count": 0,
                    "avg_processing_time": 0.0,
                    "last_updated": datetime.now(timezone.utc),
                    "errors": 0
                }

                # Create stop event
                stop_event = asyncio.Event()

                # Create processing task
                task = asyncio.create_task(
                    self.processing_service.process_stream_with_sharing(
                        stream_id=stream_id,
                        camera_name=camera_name,
                        source=source,
                        owner_username=owner_username,
                        owner_id=owner_id,
                        workspace_id=workspace_id,
                        stop_event=stop_event,
                        location_info=location_info
                    )
                )
                task.set_name(f"process_stream_{stream_id_str}")
                task.add_done_callback(
                    lambda t: asyncio.create_task(
                        self._handle_stream_task_completion(stream_id_str, t)
                    )
                )

                # Update stream info
                async with self._lock:
                    self.active_streams[stream_id_str].update({
                        'stop_event': stop_event,
                        'task': task,
                        'status': 'active_pending'
                    })
                
                # Register with shared stream manager
                self._shared_stream_registry[source].add(stream_id_str)
                
                # Transition to ACTIVE state
                await self._transition_stream_state(stream_id_str, StreamState.ACTIVE)
                
                # Ensure Qdrant collection
                asyncio.create_task(self._ensure_collection_for_stream_workspace(workspace_id))
                
                # Notify workspace members
                await self._notify_workspace_stream_started(workspace_id, camera_name, owner_username)
                
                # Update metrics
                self.metrics['total_streams_started'] += 1
                
                logger.info(f"✅ Stream {stream_id_str} started in workspace {workspace_id_str}")

            except Exception as e:
                logger.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
                
                # Cleanup on failure
                async with self._lock:
                    self.active_streams.pop(stream_id_str, None)
                    self.workspace_streams[workspace_id_str].discard(stream_id_str)
                    self.stream_workspaces.pop(stream_id_str, None)
                
                self.stream_processing_stats.pop(stream_id_str, None)
                self._shared_stream_registry[source].discard(stream_id_str)
                
                # Update state
                await self._transition_stream_state(stream_id_str, StreamState.ERROR, force=True)
                
                # Update database
                await self.video_stream_service.update_stream_status(
                    stream_id, 'error', is_streaming=False
                )
                
                raise

    async def _handle_stream_task_completion(self, stream_id_str: str, task: asyncio.Task):
        """
        Handle stream task completion with proper cleanup.
        NEW: Ensures cleanup even on unexpected termination.
        """
        try:
            if task.cancelled():
                logger.info(f"Stream {stream_id_str} task was cancelled")
            elif task.exception():
                exc = task.exception()
                logger.error(f"Stream {stream_id_str} task failed: {exc}", exc_info=exc)
                await self._record_stream_error(stream_id_str, "task_execution", str(exc))
            else:
                logger.info(f"Stream {stream_id_str} task completed normally")
            
            # Ensure cleanup
            if stream_id_str in self.active_streams:
                await self._stop_stream(stream_id_str, for_restart=False)
                
        except Exception as e:
            logger.error(f"Error handling task completion for {stream_id_str}: {e}", exc_info=True)

    async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
        """
        stream stop with better cleanup and state management.
        ENHANCED: More thorough cleanup and error handling.
        """
        # Transition to STOPPING state
        await self._transition_stream_state(stream_id_str, StreamState.STOPPING, force=True)
        
        async with self._lock:
            stream_info = self.active_streams.pop(stream_id_str, None)
            
            # Clean up workspace registry
            if stream_id_str in self.stream_workspaces:
                workspace_id_str = self.stream_workspaces.pop(stream_id_str)
                if workspace_id_str in self.workspace_streams:
                    self.workspace_streams[workspace_id_str].discard(stream_id_str)
        
        if not stream_info:
            # Ensure database is updated even if stream not in memory
            if not for_restart:
                try:
                    await self.db_manager.execute_query(
                        """UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', 
                           updated_at = NOW(), last_activity = NOW() 
                           WHERE stream_id = $1 AND is_streaming = TRUE""",
                        (UUID(stream_id_str),)
                    )
                except Exception as e:
                    logger.error(f"Error updating database for non-existent stream {stream_id_str}: {e}")
            
            await self._transition_stream_state(stream_id_str, StreamState.INACTIVE, force=True)
            return

        stream_uuid = UUID(stream_id_str)
        stop_event_obj = stream_info.get('stop_event')
        task_obj = stream_info.get('task')
        workspace_id = stream_info.get('workspace_id')
        camera_name = stream_info.get('camera_name', 'Unknown Camera')
        source = stream_info.get('source')

        # Clean up shared stream registry
        if source:
            self._shared_stream_registry[source].discard(stream_id_str)
            if not self._shared_stream_registry[source]:
                del self._shared_stream_registry[source]

        # Clean up state
        self.fire_detection_states.pop(stream_id_str, None)
        self.fire_detection_frame_counts.pop(stream_id_str, None)
        self.people_count_notification_cooldowns.pop(stream_id_str, None)
        self.stream_errors.pop(stream_id_str, None)

        # Calculate stream duration for metrics
        start_time = stream_info.get('start_time')
        if start_time:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            # Update running average
            current_avg = self.metrics['avg_stream_duration']
            total_stopped = self.metrics['total_streams_stopped']
            self.metrics['avg_stream_duration'] = (
                (current_avg * total_stopped + duration) / (total_stopped + 1)
            )

        try:
            # Signal stop
            if stop_event_obj:
                stop_event_obj.set()
            
            # Cancel task with timeout
            if task_obj and not task_obj.done():
                task_obj.cancel()
                try:
                    await asyncio.wait_for(
                        task_obj, 
                        timeout=config.get("stream_stop_timeout_seconds", 5.0)
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                    logger.warning(f"Task cancellation timeout for {stream_id_str}: {e}")
            
            now_utc = datetime.now(timezone.utc)
            
            # Update database
            if for_restart:
                await self.db_manager.execute_query(
                    """UPDATE video_stream SET status = 'processing', last_activity = $1, 
                       updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE""",
                    (now_utc, stream_uuid)
                )
            else:
                await self.db_manager.execute_query(
                    """UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', 
                       last_activity = $1, updated_at = $1 WHERE stream_id = $2""",
                    (now_utc, stream_uuid)
                )
                
                # Notify workspace members
                if workspace_id:
                    await self._notify_workspace_stream_stopped(
                        workspace_id, camera_name, stream_info.get('username')
                    )
                
                # Update metrics
                self.metrics['total_streams_stopped'] += 1
                
        except Exception as e:
            logger.error(f"Error during _stop_stream for {stream_id_str}: {e}", exc_info=True)
            await self._record_stream_error(stream_id_str, "stop", str(e))
        finally:
            self.stream_processing_stats.pop(stream_id_str, None)
            
            # Transition to final state
            if for_restart:
                await self._transition_stream_state(stream_id_str, StreamState.STARTING, force=True)
            else:
                await self._transition_stream_state(stream_id_str, StreamState.INACTIVE, force=True)

    # ==================== Workspace Notifications ====================

    async def _notify_workspace_stream_started(
        self,
        workspace_id: UUID,
        camera_name: str,
        owner_username: str
    ):
        """Notify workspace members when a stream starts."""
        try:
            # Get workspace members
            members = await self.workspace_service.get_workspace_members(
                workspace_id=workspace_id,
                current_user_id=workspace_id,
                is_admin=True
            )
            
            # Create notifications for members
            notification_tasks = [
                self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"📹 Camera '{camera_name}' started by {owner_username}",
                    camera_name=camera_name
                )
                for member in members
            ]
            
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error notifying workspace of stream start: {e}")

    async def _notify_workspace_stream_stopped(
        self,
        workspace_id: UUID,
        camera_name: str,
        owner_username: str
    ):
        """Notify workspace members when a stream stops."""
        try:
            members = await self.workspace_service.get_workspace_members(
                workspace_id=workspace_id,
                current_user_id=workspace_id,
                is_admin=True
            )
            
            notification_tasks = [
                self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"⏹️ Camera '{camera_name}' stopped (owner: {owner_username})",
                    camera_name=camera_name
                )
                for member in members
            ]
            
            await asyncio.gather(*notification_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error notifying workspace of stream stop: {e}")

    # ==================== API Methods ====================
    
    async def start_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """Start stream with workspace validation."""
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None
        )
        
        workspace_id = stream_info['workspace_id']
        
        # Check if can start
        can_start, reason = await self.can_start_stream_in_workspace(
            workspace_id, requester_user_id
        )
        
        if not can_start:
            raise HTTPException(status_code=403, detail=reason)
        
        # Mark stream for processing
        await self.db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = NOW() WHERE stream_id = $1",
            (stream_id,)
        )
        
        logger.info(f"✅ User {requester_user_id} requested start for stream {stream_id}")
        
        return {
            "stream_id": str(stream_id),
            "name": stream_info['name'],
            "workspace_id": str(workspace_id),
            "message": "Stream start initiated"
        }

    async def stop_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """Stop stream with workspace validation."""
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None
        )
        
        # Check if user owns the stream or is workspace admin
        is_owner = stream_info['user_id'] == requester_user_id
        is_workspace_admin = membership_info['role'] in ['admin', 'owner']
        
        if not is_owner and not is_workspace_admin:
            raise HTTPException(
                status_code=403,
                detail="Only stream owner or workspace admin can stop streams"
            )
        
        # Mark stream for stopping
        await self.db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = NOW() WHERE stream_id = $1",
            (stream_id,)
        )
        
        logger.info(f"✅ User {requester_user_id} requested stop for stream {stream_id}")
        
        return {
            "stream_id": str(stream_id),
            "name": stream_info['name'],
            "workspace_id": str(stream_info['workspace_id']),
            "message": "Stream stop initiated"
        }

    async def get_workspace_streams_for_user(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Get all streams accessible to a user through their workspaces."""
        if workspace_id:
            await self.workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=workspace_id
            )
            target_workspaces = [workspace_id]
        else:
            user_workspaces = await self.workspace_service.get_user_workspaces(user_id)
            target_workspaces = [UUID(ws['workspace_id']) for ws in user_workspaces]
        
        if not target_workspaces:
            return {"streams": [], "total": 0, "workspaces": []}
        
        all_streams = []
        workspace_summaries = []
        
        for ws_id in target_workspaces:
            ws_info = await self.workspace_service.get_workspace_by_id(ws_id)
            streams = await self.video_stream_service.get_workspace_streams(ws_id)
            active_streams = await self.get_workspace_active_streams(ws_id)
            
            for stream in streams:
                stream_id_str = str(stream['stream_id'])
                is_active = any(s['stream_id'] == stream_id_str for s in active_streams)
                
                all_streams.append({
                    "stream_id": stream_id_str,
                    "name": stream['name'],
                    "type": stream['type'],
                    "status": stream['status'],
                    "is_streaming": stream['is_streaming'],
                    "is_active_in_memory": is_active,
                    "workspace_id": str(ws_id),
                    "workspace_name": ws_info['name'],
                    "owner_id": str(stream['user_id']),
                    "location": stream.get('location'),
                    "created_at": stream['created_at'].isoformat() if stream['created_at'] else None
                })
            
            workspace_summaries.append({
                "workspace_id": str(ws_id),
                "workspace_name": ws_info['name'],
                "total_streams": len(streams),
                "active_streams": len([s for s in streams if s['is_streaming']]),
                "in_memory_streams": len(active_streams)
            })
        
        return {
            "streams": all_streams,
            "total": len(all_streams),
            "workspaces": workspace_summaries
        }

    # ==================== Background Tasks ====================

    async def start_background_tasks(self):
        """
        Start background tasks with monitoring.
        ENHANCED: Added resource monitoring task.
        """
        try:
            logging.info("Starting StreamManager background tasks...")
            await self._start_background_tasks_internal()
            logging.info("✅ StreamManager background tasks started successfully")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to start StreamManager background tasks: {e}", exc_info=True)
            return False

    async def _start_background_tasks_internal(self):
        """
        Internal method to start all background tasks.
        ENHANCED: Added monitoring task.
        """
        await self.stop_background_tasks()
        
        # Stream management task
        self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
        self.background_task.set_name("manage_streams_loop")
        self.background_task.add_done_callback(self._handle_task_done)
        
        # Cleanup task
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.cleanup_task.set_name("periodic_cleanup_loop")
        self.cleanup_task.add_done_callback(self._handle_task_done)
        
        # NEW: Resource monitoring task
        self.monitor_task = asyncio.create_task(self._resource_monitor())
        self.monitor_task.set_name("resource_monitor_loop")
        self.monitor_task.add_done_callback(self._handle_task_done)

    async def _resource_monitor(self):
        """
        NEW: Monitor resource usage and performance.
        Helps identify memory leaks and performance issues.
        """
        while True:
            try:
                await asyncio.sleep(config.get("resource_monitor_interval", 300))  # 5 minutes
                
                # Collect metrics
                metrics = {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'active_streams': len(self.active_streams),
                    'total_workspaces': len(self.workspace_streams),
                    'shared_sources': len(self._shared_stream_registry),
                    'notification_subscribers': sum(len(ws) for ws in self.notification_subscribers.values()),
                    'error_records': sum(len(errors) for errors in self.stream_errors.values()),
                    'performance': {
                        'total_started': self.metrics['total_streams_started'],
                        'total_stopped': self.metrics['total_streams_stopped'],
                        'total_errors': self.metrics['total_errors'],
                        'avg_duration': self.metrics['avg_stream_duration']
                    }
                }
                
                logger.info(f"📊 Resource Monitor: {metrics}")
                
                # Check for potential issues
                if len(self.active_streams) > self.max_concurrent_streams * 0.9:
                    logger.warning("⚠️ Approaching maximum concurrent streams limit!")
                
                if metrics['error_records'] > 100:
                    logger.warning(f"⚠️ High error count: {metrics['error_records']} records")
                
            except asyncio.CancelledError:
                logging.info("Resource monitor task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in resource monitor: {e}", exc_info=True)

    async def manage_streams_with_deduplication(self):
        """
        stream management loop with better error handling.
        ENHANCED: Better batching, error recovery, and workspace limits.
        """
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                # Get streams that should be running with workspace context
                streams_to_run_query = """
                    SELECT vs.stream_id, vs.name, vs.path, vs.user_id, vs.workspace_id, 
                           u.username, u.role as user_role, u.is_subscribed, u.is_active,
                           w.is_active as workspace_active, w.name as workspace_name,
                           vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                           vs.latitude, vs.longitude
                    FROM video_stream vs
                    JOIN users u ON vs.user_id = u.user_id
                    JOIN workspaces w ON vs.workspace_id = w.workspace_id
                    WHERE vs.is_streaming = TRUE 
                          AND u.is_active = TRUE
                          AND w.is_active = TRUE
                          AND (u.is_subscribed = TRUE OR u.role = 'admin')
                """
                potential_streams_db = await self.db_manager.execute_query(
                    streams_to_run_query, fetch_all=True
                )
                potential_streams_db = potential_streams_db or []

                async with self._lock:
                    current_running_ids_mem = set(self.active_streams.keys())

                db_should_run_ids = {str(s['stream_id']) for s in potential_streams_db}

                # Group streams by workspace and file path
                workspace_streams_map = defaultdict(lambda: defaultdict(list))
                for stream_data in potential_streams_db:
                    ws_id = str(stream_data['workspace_id'])
                    file_path = stream_data['path']
                    workspace_streams_map[ws_id][file_path].append(stream_data)

                # Track start requests for batching
                start_tasks = []

                # Process each workspace
                for workspace_id_str, paths_map in workspace_streams_map.items():
                    try:
                        workspace_id = UUID(workspace_id_str)
                        
                        # Check workspace limits (with caching)
                        limits = await self.get_workspace_stream_limits(workspace_id)
                        
                        if limits['available_slots'] <= 0:
                            logger.warning(
                                f"⚠️ Workspace {workspace_id_str} at capacity "
                                f"({limits['current_active']}/{limits['total_camera_limit']})"
                            )
                            continue
                        
                        # Process each file path in workspace
                        for file_path, streams_for_path in paths_map.items():
                            # Sort by priority (admin first, then by stream_id for consistency)
                            streams_for_path.sort(key=lambda x: (
                                x.get('user_role') != 'admin',
                                x['stream_id']
                            ))
                            
                            enable_sharing = config.get("enable_stream_sharing", True)
                            max_streams_per_file = config.get(
                                "max_streams_per_file", 
                                5 if enable_sharing else 1
                            )
                            
                            active_streams_for_path = [
                                s for s in streams_for_path
                                if str(s['stream_id']) in current_running_ids_mem
                            ]
                            
                            can_start_count = max_streams_per_file - len(active_streams_for_path)
                            started_count = 0
                            
                            for stream_data in streams_for_path:
                                stream_id_str = str(stream_data['stream_id'])
                                
                                # Skip if already running
                                if stream_id_str in current_running_ids_mem:
                                    continue
                                
                                # Skip if reached file limit
                                if started_count >= can_start_count:
                                    logger.debug(
                                        f"File {file_path} reached concurrent limit "
                                        f"({max_streams_per_file})"
                                    )
                                    break
                                
                                # Skip if reached workspace limit
                                if limits['available_slots'] <= started_count:
                                    logger.debug(
                                        f"Workspace {workspace_id_str} reached limit, "
                                        f"deferring stream {stream_id_str}"
                                    )
                                    break
                                
                                # Double-check user limits
                                owner_id_obj = stream_data['user_id']
                                can_start, reason = await self.can_start_stream_in_workspace(
                                    workspace_id, owner_id_obj
                                )
                                
                                if not can_start:
                                    logger.warning(
                                        f"Cannot start stream {stream_id_str}: {reason}"
                                    )
                                    # Update database to reflect inability to start
                                    await self.video_stream_service.update_stream_status(
                                        stream_data['stream_id'], 'inactive', is_streaming=False
                                    )
                                    continue

                                # Prepare location info
                                location_info = {
                                    'location': stream_data.get('location'),
                                    'area': stream_data.get('area'),
                                    'building': stream_data.get('building'),
                                    'zone': stream_data.get('zone'),
                                    'floor_level': stream_data.get('floor_level'),
                                    'latitude': stream_data.get('latitude'),
                                    'longitude': stream_data.get('longitude')
                                }
                                
                                # Create start task
                                task = self.start_stream_background(
                                    stream_data['stream_id'],
                                    owner_id_obj,
                                    stream_data['username'],
                                    stream_data['name'],
                                    stream_data['path'],
                                    workspace_id,
                                    location_info
                                )
                                start_tasks.append(task)
                                started_count += 1
                    
                    except Exception as e:
                        logger.error(
                            f"Error processing workspace {workspace_id_str}: {e}", 
                            exc_info=True
                        )
                        consecutive_errors += 1

                # Execute all start tasks concurrently (with limit)
                if start_tasks:
                    batch_size = config.get("stream_start_batch_size", 10)
                    for i in range(0, len(start_tasks), batch_size):
                        batch = start_tasks[i:i + batch_size]
                        results = await asyncio.gather(*batch, return_exceptions=True)
                        
                        # Log any errors
                        for idx, result in enumerate(results):
                            if isinstance(result, Exception):
                                logger.error(
                                    f"Error starting stream in batch: {result}", 
                                    exc_info=result
                                )

                # Stop streams that should no longer be running
                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                if streams_to_stop_ids:
                    logger.info(f"Stopping {len(streams_to_stop_ids)} streams")
                    stop_tasks = [
                        self._stop_stream(stream_id_str, for_restart=False)
                        for stream_id_str in streams_to_stop_ids
                    ]
                    await asyncio.gather(*stop_tasks, return_exceptions=True)

                # Cleanup empty shared streams
                self.video_file_manager.cleanup_empty_streams()

                # Health check
                current_time = datetime.now(timezone.utc)
                if (current_time - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
                
                # Reset error counter on success
                consecutive_errors = 0
                
            except asyncio.CancelledError:
                logging.info("Manage streams task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in manage_streams loop: {e}", exc_info=True)
                consecutive_errors += 1
                
                # If too many consecutive errors, increase sleep time
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"⚠️ {consecutive_errors} consecutive errors in manage_streams. "
                        f"Backing off..."
                    )
                    await asyncio.sleep(30)  # Back off for 30 seconds
                    consecutive_errors = 0  # Reset after backoff
            
            # Dynamic sleep based on load
            active_count = len(self.active_streams)
            if active_count > 50:
                sleep_time = 2.0  # More frequent checks when busy
            elif active_count > 20:
                sleep_time = 3.0
            else:
                sleep_time = config.get("stream_manager_poll_interval_seconds", 5.0)
            
            await asyncio.sleep(sleep_time)

    async def _periodic_cleanup(self):
        """
        periodic cleanup with better error handling.
        ENHANCED: More thorough cleanup and better logging.
        """
        while True:
            try:
                # Clean websocket connections
                await self._clean_websocket_connections()

                # Clean up expired cooldowns
                current_time = time.time()
                
                # Fire cooldowns (24 hour expiry)
                fire_cooldown_expiry = 86400
                expired_fire_cooldowns = [
                    stream_id for stream_id, last_time in list(self.fire_detection_states.items())
                    if (current_time - last_time) > fire_cooldown_expiry 
                       and stream_id not in self.active_streams
                ]
                for key in expired_fire_cooldowns:
                    self.fire_detection_states.pop(key, None)
                
                # People count cooldowns (24 hour expiry)
                people_cooldown_expiry = 86400
                expired_people_cooldowns = [
                    stream_id for stream_id, last_time 
                    in list(self.people_count_notification_cooldowns.items())
                    if (current_time - last_time) > people_cooldown_expiry 
                       and stream_id not in self.active_streams
                ]
                for key in expired_people_cooldowns:
                    self.people_count_notification_cooldowns.pop(key, None)
                
                # Clean up old fire states in database
                await self.fire_service.cleanup_old_fire_states()

                # Clean up orphaned workspace registrations
                async with self._lock:
                    for workspace_id_str in list(self.workspace_streams.keys()):
                        # Remove streams that are no longer active
                        active_stream_ids = self.workspace_streams[workspace_id_str]
                        orphaned = [
                            sid for sid in active_stream_ids 
                            if sid not in self.active_streams
                        ]
                        for sid in orphaned:
                            self.workspace_streams[workspace_id_str].discard(sid)
                            self.stream_workspaces.pop(sid, None)
                        
                        # Remove empty workspace entries
                        if not self.workspace_streams[workspace_id_str]:
                            del self.workspace_streams[workspace_id_str]
                            logger.debug(f"Removed empty workspace registry: {workspace_id_str}")

                # Clean up orphaned shared stream registry
                for source in list(self._shared_stream_registry.keys()):
                    stream_ids = self._shared_stream_registry[source]
                    active_ids = [sid for sid in stream_ids if sid in self.active_streams]
                    
                    if not active_ids:
                        del self._shared_stream_registry[source]
                        logger.debug(f"Removed orphaned shared stream registry: {source}")
                    else:
                        self._shared_stream_registry[source] = set(active_ids)

                # Clean up old error records (keep last 100)
                for stream_id in list(self.stream_errors.keys()):
                    if len(self.stream_errors[stream_id]) > 100:
                        self.stream_errors[stream_id] = self.stream_errors[stream_id][-100:]
                    
                    # Remove error records for streams inactive for > 1 day
                    if stream_id not in self.active_streams:
                        last_error = self.stream_errors[stream_id][-1] if self.stream_errors[stream_id] else None
                        if last_error:
                            age = (datetime.now(timezone.utc) - last_error['timestamp']).total_seconds()
                            if age > 86400:  # 24 hours
                                del self.stream_errors[stream_id]

                cleanup_summary = {
                    'fire_cooldowns': len(expired_fire_cooldowns),
                    'people_cooldowns': len(expired_people_cooldowns),
                    'orphaned_registrations': len(orphaned) if 'orphaned' in locals() else 0
                }
                
                if any(cleanup_summary.values()):
                    logger.debug(f"🧹 Cleanup summary: {cleanup_summary}")

            except asyncio.CancelledError:
                logging.info("Periodic cleanup task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_cleanup_interval_seconds", 60.0))

    async def _clean_websocket_connections(self):
        """
        Clean up dead WebSocket connections.
        ENHANCED: Better logging and statistics.
        """
        cleaned_notifications = 0
        cleaned_clients = 0
        
        # Clean notification subscribers
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_ws = {
                    ws for ws in websockets 
                    if ws.client_state != WebSocketState.CONNECTED
                }
                if dead_ws:
                    self.notification_subscribers[user_id] -= dead_ws
                    cleaned_notifications += len(dead_ws)
                    
                    if not self.notification_subscribers[user_id]:
                        del self.notification_subscribers[user_id]
        
        # Clean stream clients
        async with self._lock:
            for stream_id, stream_info in list(self.active_streams.items()):
                if 'clients' in stream_info:
                    dead_clients = {
                        ws for ws in stream_info['clients'] 
                        if ws.client_state != WebSocketState.CONNECTED
                    }
                    if dead_clients:
                        stream_info['clients'] -= dead_clients
                        cleaned_clients += len(dead_clients)
        
        if cleaned_notifications or cleaned_clients:
            logger.debug(
                f"🧹 Cleaned {cleaned_notifications} notification WS, "
                f"{cleaned_clients} stream client WS"
            )

    async def _check_stream_health(self):
        """
        health check with better diagnostics.
        ENHANCED: Better detection and recovery logic.
        """
        async with self._health_lock:
            # Prevent concurrent health checks
            if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2:
                return

            current_time_utc = datetime.now(timezone.utc)
            streams_to_restart_ids = []
            health_issues = []
            
            async with self._lock:
                active_stream_ids_copy = list(self.active_streams.keys())

            for stream_id_str in active_stream_ids_copy:
                try:
                    stream_id_uuid = UUID(stream_id_str)
                    
                    # Check database state
                    db_stream_state = await self.db_manager.execute_query(
                        """SELECT vs.is_streaming, vs.status, vs.workspace_id, 
                                  w.is_active as workspace_active
                           FROM video_stream vs
                           JOIN workspaces w ON vs.workspace_id = w.workspace_id
                           WHERE vs.stream_id = $1""",
                        (stream_id_uuid,), 
                        fetch_one=True
                    )

                    if not db_stream_state:
                        health_issues.append(f"{stream_id_str}: not found in database")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Stop if workspace is inactive
                    if not db_stream_state.get('workspace_active'):
                        health_issues.append(f"{stream_id_str}: workspace inactive")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Stop if stream should not be running
                    if not db_stream_state.get('is_streaming', False):
                        health_issues.append(f"{stream_id_str}: externally stopped")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    async with self._lock:
                        stream_info_mem = self.active_streams.get(stream_id_str)
                    
                    if not stream_info_mem:
                        continue

                    # Check for stale frames
                    last_activity_time_mem = (
                        stream_info_mem.get('last_frame_time') or 
                        stream_info_mem.get('start_time')
                    )
                    time_since_last_frame = float('inf')
                    
                    if last_activity_time_mem:
                        if last_activity_time_mem.tzinfo is None:
                            last_activity_time_mem = last_activity_time_mem.replace(tzinfo=timezone.utc)
                        time_since_last_frame = (
                            current_time_utc - last_activity_time_mem
                        ).total_seconds()

                    stale_threshold = config.get("stream_stale_threshold_seconds", 120.0)
                    if time_since_last_frame > stale_threshold:
                        health_issues.append(
                            f"{stream_id_str}: frozen ({time_since_last_frame:.0f}s since last frame)"
                        )
                        streams_to_restart_ids.append(stream_id_str)
                    
                    # Check task health
                    task = stream_info_mem.get('task')
                    if task and task.done():
                        exc = task.exception()
                        if exc:
                            health_issues.append(f"{stream_id_str}: task failed - {exc}")
                            streams_to_restart_ids.append(stream_id_str)
                        
                except Exception as e:
                    logger.error(
                        f"Error during health check for stream {stream_id_str}: {e}", 
                        exc_info=True
                    )
                    health_issues.append(f"{stream_id_str}: health check error - {e}")

            # Restart frozen streams
            if streams_to_restart_ids:
                logger.warning(f"⚠️ Health check found {len(streams_to_restart_ids)} streams to restart")
                for stream_id_to_restart_str in streams_to_restart_ids:
                    logger.info(f"🔄 Restarting frozen stream: {stream_id_to_restart_str}")
                    await self._stop_stream(stream_id_to_restart_str, for_restart=True)
            
            # Log health summary
            if health_issues:
                logger.warning(f"Health check issues:\n" + "\n".join(f"  - {issue}" for issue in health_issues))
            else:
                logger.debug(f"✅ Health check passed for {len(active_stream_ids_copy)} streams")
            
            self.last_healthcheck = datetime.now(timezone.utc)

    # ==================== Frame Updates ====================
    
    def update_stream_frame(
        self,
        stream_id_str: str,
        frame: Any,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str
    ):
        """
        Update stream frame and statistics.
        Called by processing service.
        """
        if stream_id_str in self.active_streams:
            self.active_streams[stream_id_str]['latest_frame'] = frame
            self.active_streams[stream_id_str]['last_frame_time'] = datetime.now(timezone.utc)
            
            # Update processing stats
            if stream_id_str in self.stream_processing_stats:
                stats = self.stream_processing_stats[stream_id_str]
                stats['frames_processed'] += 1
                if person_count > 0:
                    stats['detection_count'] += 1
                stats['last_updated'] = datetime.now(timezone.utc)

    def get_shared_stream(self, source: str):
        """Get shared stream for a source."""
        return self.video_file_manager.get_shared_stream(source, max_subscribers=10)
    
    def create_shared_stream(self, source: str, owner_username: str, stream_id: UUID):
        """Create and register a shared stream."""
        shared_stream = self.video_file_manager.get_shared_stream(source, max_subscribers=10)
        return shared_stream
    
    def remove_shared_stream(self, source: str):
        """Remove a shared stream."""
        self.video_file_manager.remove_shared_stream(source)

    # ==================== Notification Management ====================

    async def subscribe_to_notifications(
        self, 
        user_id: str, 
        websocket: WebSocket
    ) -> bool:
        """Subscribe a WebSocket to notifications for a user."""
        async with self._notification_lock:
            self.notification_subscribers[user_id].add(websocket)
        
        logging.info(f"📡 WS client subscribed to notifications for user {user_id}")
        
        try:
            await websocket.send_json({
                "type": "subscription_confirmed",
                "for_user_id": user_id,
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
            return True
        except Exception as e:
            logger.error(f"Error sending subscription confirmation: {e}")
            await self.unsubscribe_from_notifications(user_id, websocket)
            return False

    async def unsubscribe_from_notifications(
        self, 
        user_id: str, 
        websocket: WebSocket
    ):
        """Unsubscribe a WebSocket from notifications."""
        async with self._notification_lock:
            if user_id in self.notification_subscribers:
                self.notification_subscribers[user_id].discard(websocket)
                if not self.notification_subscribers[user_id]:
                    del self.notification_subscribers[user_id]
        
        logging.info(f"📡 WS client unsubscribed from notifications for user {user_id}")

    async def connect_client_to_stream(
        self, 
        stream_id: str, 
        websocket: WebSocket
    ) -> bool:
        """Connect a client WebSocket to a stream."""
        async with self._lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]['clients'].add(websocket)
                logger.debug(f"Client connected to stream {stream_id}")
                return True
            return False

    async def disconnect_client(
        self, 
        stream_id: str, 
        websocket: WebSocket
    ):
        """Disconnect a client WebSocket from a stream."""
        async with self._lock:
            if stream_id in self.active_streams and 'clients' in self.active_streams[stream_id]:
                self.active_streams[stream_id]['clients'].discard(websocket)
                logger.debug(f"Client disconnected from stream {stream_id}")

    # ==================== Task Management ====================

    async def _restart_background_task_if_needed(self, failed_task_name: Optional[str] = None):
        """Restart failed background tasks."""
        await asyncio.sleep(config.get("stream_manager_restart_delay_seconds", 5.0))
        logging.info(f"🔄 Attempting to restart background task: {failed_task_name or 'Unknown'}")
        
        # Restart management task
        if failed_task_name == "manage_streams_loop" or not self.background_task or self.background_task.done():
            if self.background_task and not self.background_task.done():
                self.background_task.cancel()
                try:
                    await asyncio.wait_for(self.background_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
            self.background_task.set_name("manage_streams_loop")
            self.background_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted manage_streams task")

        # Restart cleanup task
        if failed_task_name == "periodic_cleanup_loop" or not self.cleanup_task or self.cleanup_task.done():
            if self.cleanup_task and not self.cleanup_task.done():
                self.cleanup_task.cancel()
                try:
                    await asyncio.wait_for(self.cleanup_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.set_name("periodic_cleanup_loop")
            self.cleanup_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted periodic_cleanup task")

        # Restart monitor task
        if failed_task_name == "resource_monitor_loop" or not self.monitor_task or self.monitor_task.done():
            if self.monitor_task and not self.monitor_task.done():
                self.monitor_task.cancel()
                try:
                    await asyncio.wait_for(self.monitor_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.monitor_task = asyncio.create_task(self._resource_monitor())
            self.monitor_task.set_name("resource_monitor_loop")
            self.monitor_task.add_done_callback(self._handle_task_done)
            logging.info("✅ Restarted resource_monitor task")

    def _handle_task_done(self, task: asyncio.Task):
        """Handle completed background tasks."""
        try:
            task_name = task.get_name()
            exception = task.exception()
            if exception:
                logging.error(f"❌ Task '{task_name}' failed: {exception}", exc_info=exception)
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task_name))
            elif task.cancelled():
                logging.info(f"⏹️ Task '{task_name}' was cancelled")
            else:
                logging.info(f"✅ Task '{task_name}' completed successfully")
        except Exception as e:
            logging.error(f"Error in _handle_task_done: {e}", exc_info=True)

    async def stop_background_tasks(self):
        """Stop all background tasks."""
        tasks_to_stop = [
            ("background_task", self.background_task),
            ("cleanup_task", self.cleanup_task),
            ("monitor_task", self.monitor_task),
        ]
        
        for name, task_instance in tasks_to_stop:
            if task_instance and not task_instance.done():
                task_name_str = task_instance.get_name() if hasattr(task_instance, 'get_name') else name
                try:
                    task_instance.cancel()
                    await asyncio.wait_for(task_instance, timeout=5.0)
                    logging.info(f"✅ Task {name} ({task_name_str}) cancelled successfully")
                except asyncio.TimeoutError:
                    logging.warning(f"⚠️ Timeout cancelling {name} ({task_name_str})")
                except asyncio.CancelledError:
                    logging.info(f"✅ Task {name} ({task_name_str}) was already cancelled")
                except Exception as e:
                    logging.error(f"❌ Error cancelling {name} ({task_name_str}): {e}", exc_info=True)
        
        self.background_task = None
        self.cleanup_task = None
        self.monitor_task = None

    async def _ensure_collection_for_stream_workspace(self, workspace_id: UUID):
        """Ensure Qdrant collection exists for workspace."""
        try:
            await self.qdrant_service.ensure_workspace_collection(workspace_id)
        except Exception as e:
            logger.error(f"Error ensuring Qdrant collection for workspace {workspace_id}: {e}")

    # ==================== Shutdown ====================

    async def shutdown(self):
        """shutdown with better cleanup."""
        logging.info("🛑 Shutting down StreamManager...")
        
        # Stop background tasks first
        await self.stop_background_tasks()
        
        # Get all active streams
        async with self._lock:
            active_stream_ids = list(self.active_streams.keys())
        
        # Stop all streams with timeout
        if active_stream_ids:
            logging.info(f"Stopping {len(active_stream_ids)} active streams...")
            stop_tasks = [
                self._stop_stream(stream_id, for_restart=False) 
                for stream_id in active_stream_ids
            ]
            
            try:
                await asyncio.wait_for(
                    asyncio.gather(*stop_tasks, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logging.warning("⚠️ Stream shutdown timed out after 30s")
        
        # Clear all state
        async with self._lock:
            self.active_streams.clear()
            self.workspace_streams.clear()
            self.stream_workspaces.clear()
            self.stream_states.clear()
        
        self.stream_processing_stats.clear()
        self.stream_errors.clear()
        self._shared_stream_registry.clear()
        
        # Stop shared streams
        for source in list(self.video_file_manager.shared_streams.keys()):
            self.video_file_manager.remove_shared_stream(source)
        
        logging.info("✅ StreamManager shutdown complete")


# ==================== Global Instance ====================

stream_manager = StreamManager()


async def initialize_stream_manager():
    """Initialize the stream manager."""
    try:
        await stream_manager.start_background_tasks()
        logging.info("✅ StreamManager initialized successfully")
    except Exception as e:
        logging.error(f"❌ Failed to initialize StreamManager: {e}", exc_info=True)
        asyncio.create_task(
            stream_manager._restart_background_task_if_needed("initialization_failure")
        )
