# app/services/stream_service.py
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, Set
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
# from app.services.qdrant_service import qdrant_service
from app.services.unified_data_service import unified_data_service as qdrant_service
from app.services.shared_stream_service import video_file_manager
from app.services.stream_processing_service import stream_processing_service
from app.config.settings import config
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)


class StreamManager:
    """
    Centralized stream manager - coordinates all stream-related services.
    Follows the service layer pattern for clean separation of concerns.
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
        self.processing_service = stream_processing_service
        
        # Stream state management
        self._lock = asyncio.Lock()
        self._notification_lock = asyncio.Lock()
        self._health_lock = asyncio.Lock()
        
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.stream_processing_stats: Dict[str, Dict[str, Any]] = {}
        
        # Notification management
        self.notification_subscribers: Dict[str, Set[WebSocket]] = defaultdict(set)
        
        # Cooldown tracking (in-memory cache)
        self.people_count_notification_cooldowns: Dict[str, float] = {}
        self.people_count_cooldown_duration = config.get("people_count_cooldown_seconds", 300.0)
        
        # Fire detection tracking (in-memory cache - database is source of truth)
        self.fire_detection_states: Dict[str, str] = {}
        self.fire_detection_frame_counts: Dict[str, int] = {}
        self.fire_cooldown_duration = config.get("fire_cooldown_seconds", 300.0)
        
        # Health check configuration
        self.last_healthcheck = datetime.now(timezone.utc)
        self.healthcheck_interval = config.get("stream_healthcheck_interval_seconds", 60)
        
        # Background tasks
        self.background_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Initialize processing service with dependencies
        self.processing_service.initialize(
            stream_manager=self,
            video_file_manager=self.video_file_manager,
            qdrant_service=self.qdrant_service
        )
        
        logging.info("StreamManager initialized - waiting for start_background_tasks()")

    # ==================== Lifecycle Management ====================

    async def start_background_tasks(self):
        """Start background tasks for stream management."""
        try:
            logging.info("Starting StreamManager background tasks...")
            await self._start_background_tasks_internal()
            logging.info("StreamManager background tasks started successfully.")
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
        
        logging.info("StreamManager background tasks initialized.")

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
                    logging.info(f"Task {name} ({task_name_str}) cancelled successfully.")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout cancelling {name} ({task_name_str}).")
                except asyncio.CancelledError:
                    logging.info(f"Task {name} ({task_name_str}) was already cancelled/completed.")
                except Exception as e:
                    logging.error(f"Error cancelling {name} ({task_name_str}): {e}", exc_info=True)
        
        self.background_task = None
        self.cleanup_task = None

    def _handle_task_done(self, task: asyncio.Task):
        """Handle completed background tasks."""
        try:
            task_name = task.get_name()
            exception = task.exception()
            if exception:
                logging.error(f"Task '{task_name}' failed: {exception}", exc_info=exception)
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task_name))
            elif task.cancelled():
                logging.info(f"Task '{task_name}' was cancelled.")
            else:
                logging.info(f"Task '{task_name}' completed successfully.")
        except Exception as e:
            logging.error(f"Error in _handle_task_done for task {task.get_name()}: {e}", exc_info=True)
            if task.done() and not task.cancelled() and task.exception():
                asyncio.create_task(self._restart_background_task_if_needed(failed_task_name=task.get_name()))

    async def _restart_background_task_if_needed(self, failed_task_name: Optional[str] = None):
        """Restart failed background tasks."""
        await asyncio.sleep(config.get("stream_manager_restart_delay_seconds", 5.0))
        logging.info(f"Attempting to restart background task(s), original failure (if any) in: {failed_task_name or 'Unknown'}")
        
        # Check and restart manage_streams task
        needs_restart_manage_streams = (
            failed_task_name == "manage_streams_loop" or 
            not self.background_task or 
            self.background_task.done()
        )
        
        if needs_restart_manage_streams:
            if self.background_task and not self.background_task.done():
                self.background_task.cancel()
                try:
                    await asyncio.wait_for(self.background_task, timeout=config.get("task_cancel_timeout_seconds", 5.0))
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.background_task = asyncio.create_task(self.manage_streams_with_deduplication())
            self.background_task.set_name("manage_streams_loop")
            self.background_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted manage_streams task.")

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
                    await asyncio.wait_for(self.cleanup_task, timeout=config.get("task_cancel_timeout_seconds", 5.0))
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.cleanup_task.set_name("periodic_cleanup_loop")
            self.cleanup_task.add_done_callback(self._handle_task_done)
            logging.info("Restarted periodic_cleanup task.")

    async def shutdown(self):
        """Shutdown the stream manager."""
        logging.info("Shutting down StreamManager...")
        await self.stop_background_tasks()
        
        async with self._lock:
            active_stream_ids = list(self.active_streams.keys())
        
        stop_tasks = [self._stop_stream(stream_id, for_restart=False) for stream_id in active_stream_ids]
        await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        async with self._lock:
            self.active_streams.clear()
        self.stream_processing_stats.clear()

        # Stop all shared streams
        for source in list(self.video_file_manager.shared_streams.keys()):
            self.video_file_manager.remove_shared_stream(source)
        
        logging.info("StreamManager shutdown complete.")

    # ==================== Stream Lifecycle ====================

    async def start_stream_background(self, stream_id: UUID, owner_id: UUID, owner_username: str,
                                     camera_name: str, source: str, workspace_id: UUID,
                                     location_info: Optional[Dict[str, Any]] = None):
        """Start stream processing in background."""
        stream_id_str = str(stream_id)
        
        async with self._lock:
            if stream_id_str in self.active_streams:
                logging.info(f"Stream {stream_id_str} already active or being started.")
                return
            
            # Initialize with 'starting' status
            self.active_streams[stream_id_str] = {
                'status': 'starting',
                'task': None,
                'start_time': datetime.now(timezone.utc),
                'location_info': location_info or {},
                'source': source,
                'camera_name': camera_name,
                'username': owner_username,
                'user_id': owner_id,
                'workspace_id': workspace_id,
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

            # Create processing task using the processing service
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

            # Add done callback to detect task failures
            def task_done_callback(t):
                try:
                    if t.exception():
                        logging.error(f"Stream processing task for {stream_id_str} failed: {t.exception()}", exc_info=t.exception())
                        # Mark stream as error in database
                        asyncio.create_task(self._handle_stream_task_failure(stream_id_str, stream_id, workspace_id, owner_id, camera_name))
                    elif t.cancelled():
                        logging.info(f"Stream processing task for {stream_id_str} was cancelled")
                    else:
                        logging.info(f"Stream processing task for {stream_id_str} completed normally")
                except Exception as e:
                    logging.error(f"Error in task_done_callback for {stream_id_str}: {e}", exc_info=True)
            
            task.add_done_callback(task_done_callback)

            async with self._lock:
                if stream_id_str in self.active_streams: 
                    self.active_streams[stream_id_str].update({
                        'stop_event': stop_event,
                        'clients': set(),
                        'latest_frame': None,
                        'last_frame_time': datetime.now(timezone.utc),
                        'task': task,
                        'status': 'active_pending',  
                    })
            
            asyncio.create_task(self._ensure_collection_for_stream_workspace(workspace_id))
            logging.info(f"Background processing task created for stream {stream_id_str} ({camera_name})")

        except Exception as e:
            logging.error(f"Failed to start stream {stream_id_str}: {e}", exc_info=True)
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            self.stream_processing_stats.pop(stream_id_str, None)
            
            await self.video_stream_service.update_stream_status(
                stream_id, 'error', is_streaming=False
            )
            
            await self.notification_service.create_notification(
                workspace_id=workspace_id,
                user_id=owner_id,
                status="error",
                message="Failed to initiate stream processing.",
                stream_id=stream_id,
                camera_name=camera_name
            )

    async def _stop_stream(self, stream_id_str: str, for_restart: bool = False):
        """Stop a stream."""
        async with self._lock:
            stream_info = self.active_streams.pop(stream_id_str, None)
        
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

        # Clean up in-memory state
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
                except asyncio.CancelledError:
                    logging.debug(f"Stream task for {stream_id_str} cancelled as expected.")
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout stopping stream task {stream_id_str}.")
            
            logging.info(f"Stream {stream_id_str} processing task signaled to stop.")
            now_utc = datetime.now(timezone.utc)
            
            if for_restart:
                await self.db_manager.execute_query(
                    """UPDATE video_stream SET status = 'processing', last_activity = $1, 
                       updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE""",
                    (now_utc, stream_uuid)
                )
                logging.info(f"Stream {stream_id_str} marked 'processing' for restart.")
            else:
                await self.db_manager.execute_query(
                    """UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', 
                       last_activity = $1, updated_at = $1 WHERE stream_id = $2""",
                    (now_utc, stream_uuid)
                )
                
                owner_id = stream_info.get('user_id')
                ws_id = stream_info.get('workspace_id')
                cam_name = stream_info.get('camera_name', 'Unknown Camera')
                
                if owner_id and ws_id:
                    await self.notification_service.create_notification(
                        workspace_id=ws_id,
                        user_id=owner_id,
                        status="inactive",
                        message=f"Camera '{cam_name}' was stopped.",
                        stream_id=UUID(stream_id_str),
                        camera_name=cam_name
                    )
        except Exception as e:
            logging.error(f"Error during _stop_stream for {stream_id_str}: {e}", exc_info=True)
        finally:
            self.stream_processing_stats.pop(stream_id_str, None)

    # ==================== Stream Management Loop ====================

    async def manage_streams_with_deduplication(self):
        """Main loop for managing streams with file sharing support."""
        while True:
            try:
                # Get streams that should be running
                streams_to_run_query = """
                    SELECT vs.stream_id, vs.name, vs.path, vs.user_id, vs.workspace_id, u.username,
                           vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                           vs.latitude, vs.longitude
                    FROM video_stream vs
                    JOIN users u ON vs.user_id = u.user_id
                    WHERE vs.is_streaming = TRUE AND u.is_active = TRUE
                          AND (u.is_subscribed = TRUE OR u.role = 'admin')
                """
                potential_streams_db = await self.db_manager.execute_query(
                    streams_to_run_query, fetch_all=True
                )
                potential_streams_db = potential_streams_db or []

                async with self._lock:
                    current_running_ids_mem = set(self.active_streams.keys())

                db_should_run_ids = {str(s['stream_id']) for s in potential_streams_db}

                # Group streams by file path
                streams_by_path = defaultdict(list)
                for stream_data in potential_streams_db:
                    streams_by_path[stream_data['path']].append(stream_data)

                # Process each file path group
                for file_path, streams_for_path in streams_by_path.items():
                    # Sort by priority
                    streams_for_path.sort(key=lambda x: (
                        x.get('user_role') != 'admin',
                        x['stream_id']
                    ))
                    
                    enable_sharing = config.get("enable_stream_sharing", True)
                    max_streams_per_file = config.get("max_streams_per_file", 5 if enable_sharing else 1)
                    
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
                            if not enable_sharing:
                                await self.video_stream_service.update_stream_status(
                                    stream_data['stream_id'], 'inactive', is_streaming=False
                                )
                            continue
                        
                        # Validate permissions
                        owner_id_obj = stream_data['user_id']
                        workspace_id_obj = stream_data['workspace_id']
                        
                        owner_info = await self.db_manager.execute_query(
                            "SELECT count_of_camera, role FROM users WHERE user_id = $1",
                            (owner_id_obj,), fetch_one=True
                        )
                        
                        if not owner_info:
                            continue
                        
                        owner_camera_limit = owner_info["count_of_camera"]
                        owner_role = owner_info["role"]
                        
                        # Check camera limits
                        active_count_res = await self.db_manager.execute_query(
                            """SELECT COUNT(*) as count FROM video_stream
                               WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE""",
                            (owner_id_obj, workspace_id_obj), fetch_one=True
                        )
                        current_owner_ws_active_count = active_count_res['count'] if active_count_res else 0

                        if owner_role != 'admin' and current_owner_ws_active_count >= owner_camera_limit:
                            logging.warning(f"Owner {stream_data['username']} at camera limit. Stream {stream_id_str} marked inactive.")
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
                                stream_data['stream_id'], owner_id_obj, stream_data['username'],
                                stream_data['name'], stream_data['path'], workspace_id_obj, location_info
                            )
                        )
                        started_count += 1

                # Stop streams that should no longer be running
                streams_to_stop_ids = current_running_ids_mem - db_should_run_ids
                for stream_id_to_stop_str in streams_to_stop_ids:
                    logging.info(f"Stopping stream {stream_id_to_stop_str}")
                    await self._stop_stream(stream_id_to_stop_str, for_restart=False)

                # Cleanup empty shared streams
                self.video_file_manager.cleanup_empty_streams()

                # Health check
                if (datetime.now(timezone.utc) - self.last_healthcheck).total_seconds() > self.healthcheck_interval:
                    await self._check_stream_health()
                
            except asyncio.CancelledError:
                logging.info("Manage streams task cancelled.")
                break
            except Exception as e:
                logging.error(f"Error in manage_streams loop: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_manager_poll_interval_seconds", 5.0))

    # ==================== Periodic Cleanup ====================

    async def _periodic_cleanup(self):
        """Periodic cleanup of cached data and stale connections."""
        while True:
            try:
                # Clean websocket connections
                await self._clean_websocket_connections()

                # Clean up expired cooldowns
                current_time = time.time()
                
                # Fire cooldowns
                expired_fire_cooldowns = [
                    stream_id for stream_id, last_time in list(self.fire_notification_cooldowns.items())
                    if (current_time - last_time) > 86400 and stream_id not in self.active_streams
                ]
                for key in expired_fire_cooldowns:
                    del self.fire_notification_cooldowns[key]
                
                # People count cooldowns
                expired_people_cooldowns = [
                    stream_id for stream_id, last_time in list(self.people_count_notification_cooldowns.items())
                    if (current_time - last_time) > 86400 and stream_id not in self.active_streams
                ]
                for key in expired_people_cooldowns:
                    del self.people_count_notification_cooldowns[key]
                
                # Clean up old fire states in database
                await self.fire_service.cleanup_old_fire_states()

                if expired_fire_cooldowns or expired_people_cooldowns:
                    logging.debug(f"Cleaned up {len(expired_fire_cooldowns)} fire and {len(expired_people_cooldowns)} people cooldowns")

            except asyncio.CancelledError:
                logging.info("Periodic cleanup task cancelled.")
                break
            except Exception as e:
                logging.error(f"Error in periodic cleanup: {e}", exc_info=True)
            
            await asyncio.sleep(config.get("stream_cleanup_interval_seconds", 60.0))

    async def _clean_websocket_connections(self):
        """Clean up dead WebSocket connections."""
        async with self._notification_lock:
            for user_id, websockets in list(self.notification_subscribers.items()):
                dead_ws = {ws for ws in websockets if ws.client_state != WebSocketState.CONNECTED}
                if dead_ws:
                    self.notification_subscribers[user_id] -= dead_ws
                    if not self.notification_subscribers[user_id]:
                        del self.notification_subscribers[user_id]
        
        async with self._lock:
            for stream_id, stream_info in list(self.active_streams.items()):
                if 'clients' in stream_info:
                    dead_clients = {ws for ws in stream_info['clients'] if ws.client_state != WebSocketState.CONNECTED}
                    if dead_clients:
                        stream_info['clients'] -= dead_clients

    # ==================== Health Check ====================

    async def _check_stream_health(self):
        """Check health of active streams and restart if needed."""
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
                    db_stream_state = await self.db_manager.execute_query(
                        "SELECT is_streaming, status FROM video_stream WHERE stream_id = $1",
                        (stream_id_uuid,), fetch_one=True
                    )

                    if not db_stream_state or not db_stream_state.get('is_streaming', False):
                        logging.info(f"Health check: Stream {stream_id_str} externally stopped.")
                        await self._stop_stream(stream_id_str, for_restart=False)
                        continue
                    
                    async with self._lock:
                        stream_info_mem = self.active_streams.get(stream_id_str)
                    
                    if not stream_info_mem:
                        continue

                    last_activity_time_mem = stream_info_mem.get('last_frame_time') or stream_info_mem.get('start_time')
                    time_since_last_frame = float('inf')
                    
                    if last_activity_time_mem:
                        if last_activity_time_mem.tzinfo is None:
                            last_activity_time_mem = last_activity_time_mem.replace(tzinfo=timezone.utc)
                        time_since_last_frame = (current_time_utc - last_activity_time_mem).total_seconds()

                    stale_threshold = config.get("stream_stale_threshold_seconds", 120.0)
                    if time_since_last_frame > stale_threshold:
                        logging.warning(f"Stream {stream_id_str} frozen. Queuing for restart.")
                        streams_to_restart_ids.append(stream_id_str)
                        
                except Exception as e:
                    logger.error(f"Error during health check for stream {stream_id_str}: {e}", exc_info=True)

            for stream_id_to_restart_str in streams_to_restart_ids:
                logging.info(f"Health check: Restarting frozen stream: {stream_id_to_restart_str}")
                await self._stop_stream(stream_id_to_restart_str, for_restart=True)
            
            self.last_healthcheck = datetime.now(timezone.utc)

    # ==================== Helper Methods ====================

    async def _ensure_collection_for_stream_workspace(self, workspace_id: UUID):
        """Ensure Qdrant collection exists for workspace."""
        await self.qdrant_service.ensure_workspace_collection(workspace_id)

    async def connect_client_to_stream(self, stream_id: str, websocket: WebSocket):
        """Connect a client WebSocket to a stream."""
        async with self._lock:
            if stream_id in self.active_streams:
                self.active_streams[stream_id]['clients'].add(websocket)
                return True
            return False

    async def disconnect_client(self, stream_id: str, websocket: WebSocket):
        """Disconnect a client WebSocket from a stream."""
        async with self._lock:
            if stream_id in self.active_streams and 'clients' in self.active_streams[stream_id]:
                self.active_streams[stream_id]['clients'].discard(websocket)

    # ==================== Notification Management ====================

    async def subscribe_to_notifications(self, user_id: str, websocket: WebSocket) -> bool:
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

    async def unsubscribe_from_notifications(self, user_id: str, websocket: WebSocket):
        """Unsubscribe a WebSocket from notifications."""
        async with self._notification_lock:
            if user_id in self.notification_subscribers:
                self.notification_subscribers[user_id].discard(websocket)
                if not self.notification_subscribers[user_id]:
                    del self.notification_subscribers[user_id]
        
        logging.info(f"WS client unsubscribed from notifications for user {user_id}")

    # ==================== API Methods ====================

    async def start_stream_in_workspace(self, stream_id_to_start_str: str, requester_user_id_str: str) -> Dict[str, Any]:
        """Start a stream in a workspace."""
        stream_id_obj = UUID(stream_id_to_start_str)
        requester_user_id_obj = UUID(requester_user_id_str)

        stream_q = """
            SELECT vs.name, vs.path, vs.user_id, vs.workspace_id, u.username as owner_username,
                   u.is_active as owner_is_active, u.is_subscribed as owner_is_subscribed, 
                   u.count_of_camera as owner_camera_limit, u.role as owner_system_role
            FROM video_stream vs JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = $1
        """
        stream_data = await self.db_manager.execute_query(stream_q, (stream_id_obj,), fetch_one=True)

        if not stream_data:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        s_workspace_id_obj = stream_data['workspace_id']
        
        # Check workspace access
        await check_workspace_access(
            self.db_manager,
            requester_user_id_obj,
            s_workspace_id_obj
        )

        s_owner_id_obj = stream_data['user_id']
        
        # Check owner's workspace membership
        owner_membership_q = "SELECT 1 FROM workspace_members WHERE user_id = $1 AND workspace_id = $2"
        if not await self.db_manager.execute_query(owner_membership_q, (s_owner_id_obj, s_workspace_id_obj), fetch_one=True):
            raise HTTPException(status_code=403, detail="Stream owner no longer in this workspace.")

        # Validate owner account status
        if not stream_data['owner_is_active'] or \
           (not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin'):
            raise HTTPException(status_code=403, detail="Stream owner's account inactive or subscription expired.")

        # Check camera limits
        active_owner_streams_q = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1 AND workspace_id = $2 AND is_streaming = TRUE"
        active_count_res = await self.db_manager.execute_query(active_owner_streams_q, (s_owner_id_obj, s_workspace_id_obj), fetch_one=True)
        active_count = active_count_res['count'] if active_count_res else 0
        
        if stream_data['owner_system_role'] != 'admin' and active_count >= stream_data['owner_camera_limit']:
            raise HTTPException(status_code=403, detail=f"Stream owner's camera limit ({stream_data['owner_camera_limit']}) reached in this workspace.")

        # Mark stream for processing
        await self.db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = NOW() WHERE stream_id = $1",
            (stream_id_obj,)
        )
        
        logging.info(f"User {requester_user_id_str} requested start for stream {stream_id_to_start_str}.")
        
        return {
            "stream_id": str(stream_id_obj),
            "name": stream_data['name'],
            "workspace_id": str(s_workspace_id_obj),
            "message": "Stream start initiated. Will be processed by the stream manager."
        }

    # ==================== Diagnostics ====================

    async def get_video_sharing_stats(self) -> Dict[str, Any]:
        """Get statistics about video sharing."""
        stats = self.video_file_manager.get_all_stats()
        
        total_subscribers = sum(len(stream_stats['subscribers']) for stream_stats in stats.values())
        active_sources = sum(1 for stream_stats in stats.values() if stream_stats['is_running'])
        
        return {
            "shared_streams": stats,
            "total_shared_sources": len(stats),
            "active_shared_sources": active_sources,
            "total_subscribers": total_subscribers,
            "average_subscribers_per_source": total_subscribers / len(stats) if stats else 0
        }

    async def diagnose_stream_activation_failure(self, stream_id_str: str) -> Dict[str, Any]:
        """Diagnose why a stream failed to become active."""
        try:
            stream_uuid = UUID(stream_id_str)
            
            # Check database state
            db_state = await self.db_manager.execute_query(
                "SELECT status, is_streaming, last_activity, updated_at FROM video_stream WHERE stream_id = $1",
                (stream_uuid,), fetch_one=True
            )
            
            # Check in-memory state
            async with self._lock:
                memory_state = self.active_streams.get(stream_id_str, {})
            
            # Check shared stream state
            shared_stream_info = {}
            for source, shared_stream in self.video_file_manager.shared_streams.items():
                if stream_id_str in shared_stream.subscribers:
                    shared_stream_info = {
                        'source': source,
                        'is_running': shared_stream.is_running,
                        'subscriber_count': len(shared_stream.subscribers),
                        'last_successful_read': getattr(shared_stream, 'last_successful_read', 0),
                        'stats': shared_stream.get_stats()
                    }
                    break
            
            return {
                'stream_id': stream_id_str,
                'db_state': db_state,
                'memory_state': {
                    'status': memory_state.get('status'),
                    'task_done': memory_state.get('task') and memory_state.get('task').done(),
                    'start_time': memory_state.get('start_time'),
                    'last_frame_time': memory_state.get('last_frame_time')
                },
                'shared_stream_info': shared_stream_info
            }
            
        except Exception as e:
            return {'error': str(e)}


    async def _handle_stream_task_failure(self, stream_id_str: str, stream_id: UUID, 
                                        workspace_id: UUID, owner_id: UUID, camera_name: str):
        """Handle stream processing task failure."""
        try:
            # Remove from active streams
            async with self._lock:
                self.active_streams.pop(stream_id_str, None)
            
            # Update database status
            await self.video_stream_service.update_stream_status(
                stream_id, 'error', is_streaming=False
            )
            
            # Create notification
            await self.notification_service.create_notification(
                workspace_id=workspace_id,
                user_id=owner_id,
                status="error",
                message=f"Stream processing failed for camera '{camera_name}'",
                stream_id=stream_id,
                camera_name=camera_name
            )
        except Exception as e:
            logging.error(f"Error handling stream task failure for {stream_id_str}: {e}", exc_info=True)


# ==================== Global Instance ====================

stream_manager = StreamManager()


async def initialize_stream_manager():
    """Initialize the stream manager background tasks."""
    try:
        await stream_manager.start_background_tasks()
        logging.info("Stream manager background tasks initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize StreamManager tasks: {e}", exc_info=True)
        asyncio.create_task(stream_manager._restart_background_task_if_needed("initialization_failure"))

