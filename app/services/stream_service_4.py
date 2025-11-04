# app/services/stream_service.py
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple
from uuid import UUID
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from fastapi import HTTPException, WebSocket
from starlette.websockets import WebSocketState

from app.services.database import db_manager
from app.services.fire_detection_service import fire_detection_service
from app.services.people_count_service import people_count_service
from app.services.notification_service import notification_service
from app.services.video_stream_service import video_stream_service
from app.services.parameter_service import parameter_service
from app.services.qdrant_service import qdrant_service
from app.services.workspace_service import workspace_service
from app.services.shared_stream_service import video_file_manager
from app.services.stream_processing_service import stream_processing_service
from app.config.settings import config
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)


class StreamManager:
    """
    centralized stream manager with deep workspace integration.
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
        
        # Stream state management
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_processing_stats: Dict[str, Dict[str, Any]] = {}
        
        # Workspace-aware stream registry
        self.workspace_streams: Dict[str, Set[str]] = defaultdict(set)  # workspace_id -> stream_ids
        self.stream_workspaces: Dict[str, str] = {}  # stream_id -> workspace_id
        
        # Notification management
        self.notification_subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        
        # Cooldown tracking
        self.people_count_notification_cooldowns: Dict[str, float] = {}
        self.people_count_cooldown_duration = config.get("people_count_cooldown_seconds", 300.0)
        
        # Fire detection tracking
        self.fire_detection_states: Dict[str, str] = {}
        self.fire_detection_frame_counts: Dict[str, int] = {}
        self.fire_cooldown_duration = config.get("fire_cooldown_seconds", 300.0)
        
        # Health check configuration
        self.last_healthcheck = datetime.now(timezone.utc)
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        
        # Background tasks
        self.background_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Shared stream management
        self._shared_stream_registry: Dict[str, List[str]] = defaultdict(list)
        
        # Initialize processing service
        self.processing_service.initialize(
            stream_manager=self,
            video_file_manager=self.video_file_manager,
            qdrant_service=self.qdrant_service
        )
        
        logging.info("StreamManager initialized with workspace integration")

    # ==================== Workspace Integration====================

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
        """Get stream limits for a workspace based on members' subscriptions."""
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
                "available_slots": 0
            }
        
        # Get current active streams
        active_streams = await self.video_stream_service.get_workspace_streams(
            workspace_id, status='active'
        )
        
        total_limit = result.get('total_camera_limit', 0) or 0
        active_count = len([s for s in active_streams if s.get('is_streaming')])
        
        return {
            "total_camera_limit": total_limit,
            "active_members": result.get('active_members', 0),
            "subscribed_members": result.get('subscribed_members', 0),
            "current_active": active_count,
            "available_slots": max(0, total_limit - active_count)
        }

    async def can_start_stream_in_workspace(
        self,
        workspace_id: UUID,
        user_id: UUID
    ) -> Tuple[bool, str]:
        """
        Check if a stream can be started in a workspace.
        Returns: (can_start, reason)
        """
        # Get workspace limits
        limits = await self.get_workspace_stream_limits(workspace_id)
        
        if limits['available_slots'] <= 0:
            return False, f"Workspace camera limit reached ({limits['total_camera_limit']})"
        
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
            return False, "User subscription expired"
        
        # Check user's personal limit within workspace
        user_active_query = """
            SELECT COUNT(*) as count FROM video_stream
            WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE
        """
        user_active = await self.db_manager.execute_query(
            user_active_query, (user_id, workspace_id), fetch_one=True
        )
        
        user_active_count = user_active['count'] if user_active else 0
        if user_info['role'] != 'admin' and user_active_count >= user_info['count_of_camera']:
            return False, f"User camera limit reached ({user_info['count_of_camera']})"
        
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
                    
                    # Filter by user if specified
                    if user_id and stream_data.get('user_id') != user_id:
                        continue
                    
                    streams_info.append({
                        'stream_id': stream_id_str,
                        'camera_name': stream_data.get('camera_name'),
                        'status': stream_data.get('status'),
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
        """Get comprehensive analytics for workspace streams."""
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
        for stream in active_streams:
            stats = stream.get('stats', {})
            total_frames += stats.get('frames_processed', 0)
            total_detections += stats.get('detection_count', 0)
        
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
                'avg_detections_per_stream': total_detections / streaming_now if streaming_now > 0 else 0
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
        """stream start with workspace validation."""
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
            raise HTTPException(status_code=403, detail=reason)
        
        async with self._lock:
            if stream_id_str in self.active_streams:
                logger.info(f"Stream {stream_id_str} already active")
                return
            
            # Register stream in workspace
            self.workspace_streams[workspace_id_str].add(stream_id_str)
            self.stream_workspaces[stream_id_str] = workspace_id_str
            
            self.active_streams[stream_id_str] = {
                'status': 'starting',
                'task': None,
                'start_time': datetime.now(timezone.utc),
                'location_info': location_info or {},
                'workspace_id': workspace_id
            }

        try:
            await self.video_stream_service.update_stream_status(
                stream_id, 'processing', is_streaming=None
            )
            
            stop_event = asyncio.Event()
            self.stream_processing_stats[stream_id_str] = {
                "frames_processed": 0,
                "detection_count": 0,
                "avg_processing_time": 0.0,
                "last_updated": datetime.now(timezone.utc)
            }

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

            async with self._lock:
                self.active_streams[stream_id_str] = {
                    'source': source,
                    'stop_event': stop_event,
                    'camera_name': camera_name,
                    'username': owner_username,
                    'user_id': owner_id,
                    'workspace_id': workspace_id,
                    'clients': set(),
                    'latest_frame': None,
                    'last_frame_time': datetime.now(timezone.utc),
                    'task': task,
                    'start_time': self.active_streams[stream_id_str]['start_time'],
                    'status': 'active_pending',
                    'location_info': location_info or {}
                }
            
            # Ensure Qdrant collection
            asyncio.create_task(self._ensure_collection_for_stream_workspace(workspace_id))
            
            # Notify workspace members
            await self._notify_workspace_stream_started(workspace_id, camera_name, owner_username)
            
            logger.info(f"Stream {stream_id_str} started in workspace {workspace_id_str}")

        except Exception as e:
            logger.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
                self.workspace_streams[workspace_id_str].discard(stream_id_str)
                self.stream_workspaces.pop(stream_id_str, None)
            
            self.stream_processing_stats.pop(stream_id_str, None)
            
            await self.video_stream_service.update_stream_status(
                stream_id, 'error', is_streaming=False
            )
            raise

    async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
        """stream stop with workspace cleanup."""
        async with self._lock:
            stream_info = self.active_streams.pop(stream_id_str, None)
            
            # Clean up workspace registry
            if stream_id_str in self.stream_workspaces:
                workspace_id_str = self.stream_workspaces.pop(stream_id_str)
                if workspace_id_str in self.workspace_streams:
                    self.workspace_streams[workspace_id_str].discard(stream_id_str)
        
        if not stream_info:
            if not for_restart:
                await self.db_manager.execute_query(
                    """UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', 
                       updated_at = NOW(), last_activity = NOW() 
                       WHERE stream_id = $1 AND is_streaming = TRUE""",
                    (UUID(stream_id_str),)
                )
            return

        stream_uuid = UUID(stream_id_str)
        stop_event_obj = stream_info.get('stop_event')
        task_obj = stream_info.get('task')
        workspace_id = stream_info.get('workspace_id')
        camera_name = stream_info.get('camera_name', 'Unknown Camera')

        # Clean up state
        self.fire_detection_states.pop(stream_id_str, None)
        self.fire_detection_frame_counts.pop(stream_id_str, None)
        self.people_count_notification_cooldowns.pop(stream_id_str, None)

        try:
            if stop_event_obj:
                stop_event_obj.set()
            
            if task_obj and not task_obj.done():
                task_obj.cancel()
                try:
                    await asyncio.wait_for(task_obj, timeout=config.get("stream_stop_timeout_seconds", 5.0))
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            now_utc = datetime.now(timezone.utc)
            
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
                
        except Exception as e:
            logger.error(f"Error during _stop_stream for {stream_id_str}: {e}", exc_info=True)
        finally:
            self.stream_processing_stats.pop(stream_id_str, None)

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
                current_user_id=workspace_id,  # Pass workspace_id as user_id for system notification
                is_admin=True
            )
            
            # Create notifications for members
            for member in members:
                await self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"Camera '{camera_name}' started by {owner_username}",
                    camera_name=camera_name
                )
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
            
            for member in members:
                await self.notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=UUID(member['user_id']),
                    status="info",
                    message=f"Camera '{camera_name}' stopped (owner: {owner_username})",
                    camera_name=camera_name
                )
        except Exception as e:
            logger.error(f"Error notifying workspace of stream stop: {e}")

    # ==================== API Methods ====================

    async def start_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """stream start with workspace validation."""
        # Validate access
        stream_info, membership_info = await self.validate_workspace_stream_access(
            user_id=requester_user_id,
            stream_id=stream_id,
            required_role=None  # Any member can start streams
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
        
        logger.info(f"User {requester_user_id} requested start for stream {stream_id}")
        
        return {
            "stream_id": str(stream_id),
            "name": stream_info['name'],
            "workspace_id": str(workspace_id),
            "workspace_name": membership_info.get('workspace_name'),
            "message": "Stream start initiated"
        }

    async def stop_stream_in_workspace(
        self,
        stream_id: UUID,
        requester_user_id: UUID
    ) -> Dict[str, Any]:
        """stream stop with workspace validation."""
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
        
        logger.info(f"User {requester_user_id} requested stop for stream {stream_id}")
        
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
            # Validate access to specific workspace
            await self.workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=workspace_id
            )
            target_workspaces = [workspace_id]
        else:
            # Get all user's workspaces
            user_workspaces = await self.workspace_service.get_user_workspaces(user_id)
            target_workspaces = [UUID(ws['workspace_id']) for ws in user_workspaces]
        
        if not target_workspaces:
            return {"streams": [], "total": 0, "workspaces": []}
        
        # Get streams for each workspace
        all_streams = []
        workspace_summaries = []
        
        for ws_id in target_workspaces:
            # Get workspace info
            ws_info = await self.workspace_service.get_workspace_by_id(ws_id)
            
            # Get streams
            streams = await self.video_stream_service.get_workspace_streams(ws_id)
            
            # Get active streams from memory
            active_streams = await self.get_workspace_active_streams(ws_id)
            
            # Format streams
            for stream in streams:
                stream_id_str = str(stream['stream_id'])
                
                # Add active status
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
                    "area": stream.get('area'),
                    "building": stream.get('building'),
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

# ==================== Additional Helper Methods ====================
    
    async def start_background_tasks(self):
        """Start background tasks."""
        try:
            logging.info("Starting StreamManager background tasks...")
            await self._start_background_tasks_internal()
            logging.info("StreamManager background tasks started successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to start StreamManager background tasks: {e}", exc_info=True)
            return False

    async def _start_background_tasks_internal(self):
        """Internal method to start background tasks."""
        await self.stop_background_tasks()
        
        self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
        self.background_task.set_name("manage_streams_loop")
        self.background_task.add_done_callback(self._handle_task_done)
        
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.cleanup_task.set_name("periodic_cleanup_loop")
        self.cleanup_task.add_done_callback(self._handle_task_done)

    async def manage_streams_with_deduplication(self):
        """stream management loop with workspace awareness."""
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

                # Process each workspace
                for workspace_id_str, paths_map in workspace_streams_map.items():
                    try:
                        workspace_id = UUID(workspace_id_str)
                        
                        # Check workspace limits
                        limits = await self.get_workspace_stream_limits(workspace_id)
                        
                        if limits['available_slots'] <= 0:
                            logger.warning(
                                f"Workspace {workspace_id_str} at capacity "
                                f"({limits['current_active']}/{limits['total_camera_limit']})"
                            )
                            continue
                        
                        # Process each file path in workspace
                        for file_path, streams_for_path in paths_map.items():
                            # Sort by priority
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
                                
                                if stream_id_str in current_running_ids_mem:
                                    continue
                                
                                if started_count >= can_start_count:
                                    continue
                                
                                # Double-check user limits
                                owner_id_obj = stream_data['user_id']
                                can_start, reason = await self.can_start_stream_in_workspace(
                                    workspace_id, owner_id_obj
                                )
                                
                                if not can_start:
                                    logger.warning(
                                        f"Cannot start stream {stream_id_str}: {reason}"
                                    )
                                    await self.video_stream_service.update_stream_status(
                                        stream_data['stream_id'], 'inactive', is_streaming=False
                                    )
                                    continue

                                # Start the stream
                                location_info = {
                                    'location': stream_data.get('location'),
                                    'area': stream_data.get('area'),
                                    'building': stream_data.get('building'),
                                    'zone': stream_data.get('zone'),
                                    'floor_level': stream_data.get('floor_level'),
                                    'latitude': stream_data.get('latitude'),
                                    'longitude': stream_data.get('longitude')
                                }
                                
                                asyncio.create_task(
                                    self.start_stream_background(
                                        stream_data['stream_id'],
                                        owner_id_obj,
                                        stream_data['username'],
                                        stream_data['name'],
                                        stream_data['path'],
                                        workspace_id,
                                        location_info
                                    )
                                )
                                started_count += 1
                    
                    except Exception as e:
                        logger.error(f"Error processing workspace {workspace_id_str}: {e}", exc_info=True)

                # Stop streams that should no longer be running
                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                for stream_id_to_stop_str in streams_to_stop_ids:
                    logger.info(f"Stopping stream {stream_id_to_stop_str}")
                    await self._stop_stream(stream_id_to_stop_str, for_restart=False)

                # Cleanup empty shared streams
                self.video_file_manager.cleanup_empty_streams()

                # Health check
                if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
                
            except asyncio.CancelledError:
                logging.info("Manage streams task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in manage_streams loop: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_manager_poll_interval_seconds", 5.0))

    async def _periodic_cleanup(self):
        """periodic cleanup with workspace awareness."""
        while True:
            try:
                # Clean websocket connections
                await self._clean_websocket_connections()

                # Clean up expired cooldowns
                current_time = time.time()
                
                # Fire cooldowns
                expired_fire_cooldowns = [
                    stream_id for stream_id, last_time in list(self.fire_detection_states.items())
                    if (current_time - last_time) > 86400 and stream_id not in self.active_streams
                ]
                for key in expired_fire_cooldowns:
                    self.fire_detection_states.pop(key, None)
                
                # People count cooldowns
                expired_people_cooldowns = [
                    stream_id for stream_id, last_time 
                    in list(self.people_count_notification_cooldowns.items())
                    if (current_time - last_time) > 86400 and stream_id not in self.active_streams
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

                if expired_fire_cooldowns or expired_people_cooldowns:
                    logger.debug(
                        f"Cleaned up {len(expired_fire_cooldowns)} fire and "
                        f"{len(expired_people_cooldowns)} people cooldowns"
                    )

            except asyncio.CancelledError:
                logging.info("Periodic cleanup task cancelled")
                break
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_cleanup_interval_seconds", 60.0))

    async def _clean_websocket_connections(self):
        """Clean up dead WebSocket connections."""
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_ws = {
                    ws for ws in websockets 
                    if ws.client_state != WebSocketState.CONNECTED
                }
                if dead_ws:
                    self.notification_subscribers[user_id] -= dead_ws
                    if not self.notification_subscribers[user_id]:
                        del self.notification_subscribers[user_id]
        
        async with self._lock:
            for stream_id, stream_info in list(self.active_streams.items()):
                if 'clients' in stream_info:
                    dead_clients = {
                        ws for ws in stream_info['clients'] 
                        if ws.client_state != WebSocketState.CONNECTED
                    }
                    if dead_clients:
                        stream_info['clients'] -= dead_clients

    async def _check_stream_health(self):
        """health check with workspace awareness."""
        async with self._health_lock:
            if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() <= self.healthcheck_interval / 2:
                return

            current_time_utc = datetime.now(timezone.utc)
            streams_to_restart_ids = []
            
            async with self._lock:
                active_stream_ids_copy = list(self.active_streams.keys())

            for stream_id_str in active_stream_ids_copy:
                try:
                    stream_id_uuid = UUID(stream_id_str)
                    
                    # Check database state
                    db_stream_state = await self.db_manager.execute_query(
                        """SELECT vs.is_streaming, vs.status, vs.workspace_id, w.is_active as workspace_active
                           FROM video_stream vs
                           JOIN workspaces w ON vs.workspace_id = w.workspace_id
                           WHERE vs.stream_id = $1""",
                        (stream_id_uuid,), 
                        fetch_one=True
                    )

                    if not db_stream_state:
                        logger.warning(f"Stream {stream_id_str} not found in database")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Stop if workspace is inactive
                    if not db_stream_state.get('workspace_active'):
                        logger.info(f"Stopping stream {stream_id_str} - workspace inactive")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    # Stop if stream should not be running
                    if not db_stream_state.get('is_streaming', False):
                        logger.info(f"Stream {stream_id_str} externally stopped")
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
                        logger.warning(f"Stream {stream_id_str} frozen. Queuing for restart")
                        streams_to_restart_ids.append(stream_id_str)
                        
                except Exception as e:
                    logger.error(
                        f"Error during health check for stream {stream_id_str}: {e}", 
                        exc_info=True
                    )

            # Restart frozen streams
            for stream_id_to_restart_str in streams_to_restart_ids:
                logger.info(f"Health check: Restarting frozen stream: {stream_id_to_restart_str}")
                await self._stop_stream(stream_id_to_restart_str, for_restart=True)
            
            self.last_healthcheck = datetime.now(timezone.utc)

    # ==================== Notification Management ====================

    async def subscribe_to_notifications(
        self, 
        user_id: str, 
        websocket: WebSocket
    ) -> bool:
        """Subscribe a WebSocket to notifications for a user."""
        async with self._notification_lock:
            self.notification_subscribers[user_id].add(websocket)
        
        logging.info(f"WS client subscribed to notifications for user {user_id}")
        
        try:
            await websocket.send_json({
                "type": "subscription_confirmed",
                "for_user_id": user_id,
                "timestamp": datetime.now(timezone.utc).timestamp()
            })
            return True
        except Exception:
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
        
        logging.info(f"WS client unsubscribed from notifications for user {user_id}")

    async def connect_client_to_stream(
        self, 
        stream_id: str, 
        websocket: WebSocket
    ):
        """Connect a client WebSocket to a stream."""
        async with self._lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]['clients'].add(websocket)
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

    # ==================== Task Management ====================

    async def _restart_background_task_if_needed(self, failed_task_name: Optional[str] = None):
        """Restart failed background tasks."""
        await asyncio.sleep(config.get("stream_manager_restart_delay_seconds", 5.0))
        logging.info(f"Attempting to restart background task: {failed_task_name or 'Unknown'}")
        
        # Check and restart manage_streams task
        needs_restart_manage = (
            failed_task_name == "manage_streams_loop" or 
            not self.background_task or 
            self.background_task.done()
        )
        
        if needs_restart_manage:
            if self.background_task and not self.background_task.done():
                self.background_task.cancel()
                try:
                    await asyncio.wait_for(
                        self.background_task, 
                        timeout=config.get("task_cancel_timeout_seconds", 5.0)
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
            self.background_task.set_name("manage_streams_loop")
            self.background_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted manage_streams task")

        # Check and restart cleanup task
        needs_restart_cleanup = (
            failed_task_name == "periodic_cleanup_loop" or 
            not self.cleanup_task or 
            self.cleanup_task.done()
        )
        
        if needs_restart_cleanup:
            if self.cleanup_task and not self.cleanup_task.done():
                self.cleanup_task.cancel()
                try:
                    await asyncio.wait_for(
                        self.cleanup_task, 
                        timeout=config.get("task_cancel_timeout_seconds", 5.0)
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.set_name("periodic_cleanup_loop")
            self.cleanup_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted periodic_cleanup task")

    def _handle_task_done(self, task: asyncio.Task):
        """Handle completed background tasks."""
        try:
            task_name = task.get_name()
            exception = task.exception()
            if exception:
                logging.error(f"Task '{task_name}' failed: {exception}", exc_info=exception)
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task_name))
            elif task.cancelled():
                logging.info(f"Task '{task_name}' was cancelled")
            else:
                logging.info(f"Task '{task_name}' completed successfully")
        except Exception as e:
            logging.error(f"Error in _handle_task_done: {e}", exc_info=True)

    async def stop_background_tasks(self):
        """Stop all background tasks."""
        tasks_to_stop = [
            ("background_task", self.background_task),
            ("cleanup_task", self.cleanup_task),
        ]
        
        for name, task_instance in tasks_to_stop:
            if task_instance and not task_instance.done():
                task_name_str = task_instance.get_name() if hasattr(task_instance, 'get_name') else name
                try:
                    task_instance.cancel()
                    timeout_seconds = float(config.get("task_cancel_timeout_seconds", 5.0))
                    await asyncio.wait_for(task_instance, timeout=timeout_seconds)
                    logging.info(f"Task {name} ({task_name_str}) cancelled successfully")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout cancelling {name} ({task_name_str})")
                except asyncio.CancelledError:
                    logging.info(f"Task {name} ({task_name_str}) was already cancelled/completed")
                except Exception as e:
                    logging.error(f"Error cancelling {name} ({task_name_str}): {e}", exc_info=True)
        
        self.background_task = None
        self.cleanup_task = None

    # ==================== Diagnostics ====================

    async def get_system_diagnostics(self) -> Dict[str, Any]:
        """Get comprehensive system diagnostics with workspace breakdown."""
        # Video sharing stats
        sharing_stats = self.video_file_manager.get_all_stats()
        
        # Workspace breakdown
        workspace_breakdown = []
        for workspace_id_str in self.workspace_streams.keys():
            try:
                workspace_id = UUID(workspace_id_str)
                analytics = await self.get_workspace_stream_analytics(workspace_id)
                workspace_breakdown.append(analytics)
            except Exception as e:
                logger.error(f"Error getting analytics for workspace {workspace_id_str}: {e}")
        
        # System totals
        total_streams = sum(ws['streams']['total'] for ws in workspace_breakdown)
        total_streaming = sum(ws['streams']['streaming_now'] for ws in workspace_breakdown)
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system": {
                "total_workspaces": len(workspace_breakdown),
                "total_streams": total_streams,
                "total_streaming": total_streaming,
                "total_shared_sources": len(sharing_stats),
                "active_shared_sources": sum(
                    1 for s in sharing_stats.values() if s['is_running']
                )
            },
            "workspaces": workspace_breakdown,
            "sharing_stats": sharing_stats,
            "memory_state": {
                "active_streams_count": len(self.active_streams),
                "workspace_registrations": len(self.workspace_streams),
                "notification_subscribers": len(self.notification_subscribers)
            }
        }

    async def shutdown(self):
        """shutdown with workspace cleanup."""
        logging.info("Shutting down StreamManager...")
        await self.stop_background_tasks()
        
        async with self._lock:
            active_stream_ids = list(self.active_streams.keys())
        
        stop_tasks = [
            self._stop_stream(stream_id, for_restart=False) 
            for stream_id in active_stream_ids
        ]
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        async with self._lock:
            self.active_streams.clear()
            self.workspace_streams.clear()
            self.stream_workspaces.clear()
        
        self.stream_processing_stats.clear()
        
        # Stop shared streams
        for source in list(self.video_file_manager.shared_streams.keys()):
            self.video_file_manager.remove_shared_stream(source)
        
        logging.info("StreamManager shutdown complete")

    async def _ensure_collection_for_stream_workspace(self, workspace_id: UUID):
        """Ensure Qdrant collection exists for workspace."""
        await self.qdrant_service.ensure_workspace_collection(workspace_id)

# ==================== Global Instance ====================

stream_manager = StreamManager()


async def initialize_stream_manager():
    """Initialize the stream manager."""
    try:
        await stream_manager.start_background_tasks()
        logging.info("stream manager initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize StreamManager: {e}", exc_info=True)
        asyncio.create_task(stream_manager._restart_background_task_if_needed("initialization_failure"))


# ==================== WebSocket Utilities ====================

async def send_ping(websocket: WebSocket):
    """Send periodic pings to keep WebSocket connection alive."""
    try:
        ping_interval = float(config.get("websocket_ping_interval", 30.0))
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(ping_interval)
            if websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": datetime.now(timezone.utc).timestamp()
                    })
                except RuntimeError as e:
                    if "close message has been sent" in str(e).lower():
                        logging.debug("Ping failed: WebSocket already closing")
                        break
                    else:
                        raise
            else:
                break
    except Exception as e:
        logging.debug(f"Ping task ended: {e}")


async def _safe_close_websocket(
    websocket: WebSocket, 
    username_for_log: Optional[str] = None
):
    """Safely close a WebSocket connection."""
    try:
        current_state = websocket.client_state
        
        if current_state == WebSocketState.CONNECTED:
            await websocket.close()
        elif current_state == WebSocketState.CONNECTING:
            await asyncio.sleep(0.1)
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
            
    except RuntimeError as e:
        if any(phrase in str(e).lower() for phrase in [
            "websocket is not connected", "already closed", "close message has been sent"
        ]):
            logging.debug(f"Websocket already closed for {username_for_log}")
        else:
            logging.warning(f"RuntimeError closing websocket: {e}")
    except Exception as e:
        logging.warning(f"Unexpected error closing websocket: {e}")
    