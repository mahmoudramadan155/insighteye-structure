# app/routes/stream_router.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Query
from fastapi.responses import JSONResponse
import time
import logging
import asyncio
from uuid import UUID
from datetime import datetime, timezone
from typing import Dict, List, Optional
import concurrent.futures
import os
from fastapi.encoders import jsonable_encoder
from starlette.websockets import WebSocketState

from app.utils import frame_to_base64, check_workspace_access
from app.config.settings import config
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.services.stream_service_2 import stream_manager
from app.schemas import ThresholdSettings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/streams2", tags=["streams2"]) 

# ThreadPoolExecutor for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 1) * 2 + 4)
)


# ==================== Helper Functions ====================

async def _safe_close_websocket(websocket: WebSocket, username_for_log: Optional[str] = None):
    """Safely close a WebSocket connection with proper state checking."""
    try:
        current_state = websocket.client_state
        
        if current_state == WebSocketState.CONNECTED:
            logger.debug(f"Closing websocket for {username_for_log}, state: {current_state}")
            await websocket.close()
        elif current_state == WebSocketState.CONNECTING:
            logger.debug(f"Websocket connecting for {username_for_log}, waiting before close")
            await asyncio.sleep(0.1)
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
        elif current_state == WebSocketState.DISCONNECTED:
            logger.debug(f"Websocket already disconnected for {username_for_log}")
        elif current_state == WebSocketState.CLOSING:
            logger.debug(f"Websocket already closing for {username_for_log}")
            
    except RuntimeError as e:
        error_msg = str(e).lower()
        if any(phrase in error_msg for phrase in [
            "websocket is not connected",
            "already closed",
            "cannot call",
            "close message has been sent"
        ]):
            logger.debug(f"Websocket already closed for {username_for_log}: {e}")
        else:
            logger.warning(f"RuntimeError closing websocket for {username_for_log}: {e}")
    except Exception as e:
        logger.warning(f"Unexpected error closing websocket for {username_for_log}: {e}")


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
                        logger.debug("Ping failed: WebSocket already closing")
                        break
                    else:
                        raise
            else:
                break
    except (WebSocketDisconnect, asyncio.CancelledError, ConnectionResetError, RuntimeError):
        logger.debug("Ping task for WebSocket ended")
    except Exception as e:
        logger.error(f"Error in WebSocket ping task: {e}", exc_info=True)


async def handle_mark_read_message(message: dict, user_id_str: str, username_for_log: str, websocket: WebSocket):
    """Handle mark_read messages from WebSocket."""
    try:
        notif_ids_raw = message.get("notification_ids", [])
        if isinstance(notif_ids_raw, list) and user_id_str:
            updated_count = 0
            valid_notif_ids_to_mark: List[UUID] = []
            
            for nid_str_raw in notif_ids_raw:
                try:
                    valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                except ValueError:
                    logger.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
            
            if valid_notif_ids_to_mark:
                for nid_uuid in valid_notif_ids_to_mark:
                    try:
                        res = await db_manager.execute_query(
                            "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                            (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)),
                            return_rowcount=True
                        )
                        if res and res > 0:
                            updated_count += 1
                    except Exception as e_mark:
                        logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
            
            # Send acknowledgment
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "type": "ack_mark_read",
                    "ids": notif_ids_raw,
                    "updated_count": updated_count
                })
    except Exception as e:
        logger.error(f"Error handling mark_read for {username_for_log}: {e}")


# ==================== Stream Control Endpoints ====================

@router.post("/start_stream/{stream_id_str}")
async def start_workspace_stream_endpoint(
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Start a specific stream."""
    try:
        requester_user_id = str(current_user_data["user_id"])
        result = await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id)
        return JSONResponse(content={"status": "success", "details": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.error(f"Error in /start_stream/{stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})


@router.post("/stop_stream/{stream_id_str}")
async def stop_workspace_stream_endpoint(
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Stop a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        stream_id_uuid = UUID(stream_id_str)

        stream_info_db = await db_manager.execute_query(
            "SELECT workspace_id, name, user_id FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )

        if not stream_info_db:
            raise HTTPException(status_code=404, detail="Stream not found.")
        
        s_workspace_id, s_name, s_owner_id = stream_info_db['workspace_id'], stream_info_db['name'], stream_info_db['user_id']
        
        # Check workspace access
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            s_workspace_id,
        )

        # Stop the stream
        updated_rows = await db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
            (datetime.now(timezone.utc), stream_id_uuid), return_rowcount=True
        )
        
        if updated_rows and updated_rows > 0:
            # Add notification via stream manager
            from app.services.notification_service import notification_service
            await notification_service.create_notification(
                workspace_id=s_workspace_id,
                user_id=s_owner_id,
                status="inactive",
                message=f"Camera '{s_name}' stopped by {requester_username}.",
                stream_id=stream_id_uuid,
                camera_name=s_name
            )
            return JSONResponse(content={"status": "success", "message": f"Stream '{s_name}' stop request processed."})
        else:
            current_status_db = await db_manager.execute_query(
                "SELECT status, is_streaming FROM video_stream WHERE stream_id = $1", 
                (stream_id_uuid,), fetch_one=True
            )
            if current_status_db and not current_status_db.get('is_streaming', True):
                return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' was already stopped."})
            return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' could not be stopped or was not found active."})

    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except ValueError:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid stream ID format."})
    except Exception as e:
        logger.error(f"Error stopping stream {stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})


@router.get("/streams")
async def get_all_workspace_streams_endpoint(
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all streams accessible to the user."""
    try:
        user_id_str = str(current_user_data["user_id"])
        result = await stream_manager.get_workspace_streams(user_id_str, workspace_id)
        return JSONResponse(content={"status": "success", "data": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.error(f"Error getting workspace streams: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})


@router.get("/streams/{stream_id}")
async def get_stream_by_id(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get details for a specific stream."""
    try:
        user_id_str = str(current_user_data["user_id"])
        result = await stream_manager.get_stream_by_id(stream_id, user_id_str)
        return JSONResponse(content={"status": "success", "data": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logger.error(f"Error getting stream {stream_id}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})


# ==================== Threshold Management ====================

@router.post("/streams/{stream_id}/thresholds")
async def update_stream_thresholds(
    stream_id: str,
    settings: ThresholdSettings,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Update count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Verify stream exists and user has access
        stream_info = await db_manager.execute_query(
            "SELECT workspace_id, name FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            stream_info['workspace_id'],
        )
        
        # Validate thresholds
        if (settings.count_threshold_greater is not None and 
            settings.count_threshold_less is not None and 
            settings.count_threshold_greater <= settings.count_threshold_less):
            raise HTTPException(status_code=400, detail="Greater threshold must be higher than less threshold")
        
        # Update thresholds
        await db_manager.execute_query(
            """UPDATE video_stream 
               SET count_threshold_greater = $1, count_threshold_less = $2, 
                   alert_enabled = $3, updated_at = NOW() 
               WHERE stream_id = $4""",
            (settings.count_threshold_greater, settings.count_threshold_less,
             settings.alert_enabled, stream_id_uuid)
        )
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Thresholds updated for stream '{stream_info['name']}'",
            "settings": {
                "count_threshold_greater": settings.count_threshold_greater,
                "count_threshold_less": settings.count_threshold_less,
                "alert_enabled": settings.alert_enabled
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error updating stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/streams/{stream_id}/thresholds")
async def get_stream_thresholds(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get count thresholds for a specific stream."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        stream_id_uuid = UUID(stream_id)
        
        # Get stream info with thresholds
        stream_info = await db_manager.execute_query(
            """SELECT vs.workspace_id, vs.name, vs.count_threshold_greater, 
                      vs.count_threshold_less, vs.alert_enabled
               FROM video_stream vs WHERE vs.stream_id = $1""",
            (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_info:
            raise HTTPException(status_code=404, detail="Stream not found")
        
        # Check workspace membership
        await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            stream_info['workspace_id'],
        )
        
        return JSONResponse(content={
            "status": "success",
            "stream_id": stream_id,
            "name": stream_info['name'],
            "thresholds": {
                "count_threshold_greater": stream_info.get('count_threshold_greater'),
                "count_threshold_less": stream_info.get('count_threshold_less'),
                "alert_enabled": stream_info.get('alert_enabled', False)
            }
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid stream ID format")
    except Exception as e:
        logger.error(f"Error getting stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== WebSocket Endpoints ====================

@router.websocket("/stream")
async def websocket_workspace_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming video frames."""
    stream_id_str: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    user_id_for_log: Optional[str] = None
    websocket_closed = False

    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        stream_id_str = query_params.get("stream_id")
        
        if not stream_id_str:
            await websocket.send_json({"status": "error", "message": "stream_id is required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        # Authenticate
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
            websocket_closed = True
            await websocket.close(1008)
            return
        
        requester_user_id_str = token_data.user_id
        user_id_for_log = requester_user_id_str
        stream_id_uuid = UUID(stream_id_str)

        # Get stream details with location data
        stream_details_query = """
            SELECT vs.workspace_id, vs.user_id as owner_id, vs.name, vs.is_streaming, vs.status,
                   u.username as owner_username,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = $1
        """
        stream_details_db = await db_manager.execute_query(
            stream_details_query, (stream_id_uuid,), fetch_one=True
        )
        
        if not stream_details_db:
            await websocket.send_json({"status": "error", "message": "Stream not found."})
            websocket_closed = True
            await websocket.close(1008)
            return

        s_workspace_id = stream_details_db['workspace_id']
        s_name = stream_details_db['name']
        s_is_streaming_db = stream_details_db['is_streaming']
        s_status_db = stream_details_db['status']
        s_owner_username = stream_details_db['owner_username']
        
        # Extract location info
        location_data = {
            "location": stream_details_db.get('location'),
            "area": stream_details_db.get('area'),
            "building": stream_details_db.get('building'),
            "zone": stream_details_db.get('zone'),
            "floor_level": stream_details_db.get('floor_level')
        }

        # Check workspace access
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            s_workspace_id,
        )
        
        # Check if stream is active
        stream_is_active_in_manager = False
        async with stream_manager._lock:
            current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
            if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                stream_is_active_in_manager = True
        
        # Start stream if not active
        if not s_is_streaming_db or not stream_is_active_in_manager:
            logger.info(f"WS: Stream {stream_id_str} not active. Attempting start by {requester_user_id_str}.")
            
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']:
                await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."})
                websocket_closed = True
                await websocket.close(1008)
                return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str)
                
                # Wait for stream to become active
                for _ in range(config.get("stream_ws_start_wait_attempts", 10)):
                    async with stream_manager._lock:
                        current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True
                        break
                    await asyncio.sleep(1.0)
                    
                if not stream_is_active_in_manager:
                    logger.warning(f"Stream {stream_id_str} failed to become active for WS")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize."})
                    websocket_closed = True
                    await websocket.close(1011)
                    return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"})
                websocket_closed = True
                await websocket.close(1011)
                return

        # Connect to stream
        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."})
            websocket_closed = True
            await websocket.close(1011)
            return

        # Check connection before sending confirmation
        if websocket.client_state != WebSocketState.CONNECTED:
            logger.warning(f"WebSocket disconnected before sending confirmation for {stream_id_str}")
            return

        # Send connection confirmation with location data
        await websocket.send_json({
            "status": "connected",
            "message": f"Connected to stream: {s_name}",
            "stream_id": stream_id_str,
            "owner": s_owner_username,
            "workspace_id": str(s_workspace_id),
            "your_role": requester_role_info.get("role"),
            "location_info": location_data
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066

        # Main streaming loop
        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            
            async with stream_manager._lock:
                stream_info = stream_manager.active_streams.get(stream_id_str, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(
                            thread_pool, frame_to_base64, latest_frame_np
                        )
            
            if not stream_ok:
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({"status": "info", "message": "Stream ended or became inactive."})
                break
            
            if latest_frame_b64 and websocket.client_state == WebSocketState.CONNECTED:
                try:
                    await websocket.send_json({
                        "stream_id": stream_id_str,
                        "frame": latest_frame_b64,
                        "timestamp": datetime.now(timezone.utc).timestamp()
                    })
                except RuntimeError as e:
                    if "close message has been sent" in str(e).lower():
                        logger.debug(f"Frame send failed: WebSocket already closing for stream {stream_id_str}")
                        break
                    else:
                        raise
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logger.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
        websocket_closed = True
    except asyncio.CancelledError:
        logger.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled")
    except ValueError as ve:
        logger.warning(f"WS stream error: Invalid ID format - {ve}")
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid stream ID format."})
                websocket_closed = True
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logger.error(f"WS stream error ({stream_id_str or 'unknown'}): {e}", exc_info=True)
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error."})
                websocket_closed = True
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        if stream_id_str:
            await stream_manager.disconnect_client(stream_id_str, websocket)
        
        if not websocket_closed:
            await _safe_close_websocket(websocket, user_id_for_log)


@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
    """WebSocket endpoint for real-time notifications."""
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    websocket_closed = False
    connection_start_time = time.time()

    try:
        await websocket.accept()
        
        # Add connection delay to prevent rapid reconnections
        await asyncio.sleep(0.5)
        
        # Authenticate
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."})
            websocket_closed = True
            await websocket.close(1008)
            return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
            websocket_closed = True
            await websocket.close(1008)
            return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager.get_user_by_id(UUID(user_id_str))
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

        # Connection stability check
        if websocket.client_state != WebSocketState.CONNECTED:
            logger.warning(f"WebSocket disconnected during authentication for {username_for_log}")
            return
        
        # Connection rate limiting
        connection_duration = time.time() - connection_start_time
        if connection_duration < 2.0:
            await asyncio.sleep(2.0 - connection_duration)

        logger.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({
            "status": "connected",
            "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get initial notifications
        try:
            from app.services.notification_service import notification_service
            initial_notifications_db = await notification_service.get_user_notifications(
                UUID(user_id_str), unread_only=False, limit=20
            )
            
            if initial_notifications_db and websocket.client_state == WebSocketState.CONNECTED:
                formatted_notifications = []
                for notification in initial_notifications_db:
                    formatted_notifications.append({
                        "id": str(notification.get("notification_id")),
                        "user_id": str(notification.get("user_id")),
                        "workspace_id": str(notification.get("workspace_id")),
                        "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                        "camera_name": notification.get("camera_name"),
                        "status": notification.get("status"),
                        "message": notification.get("message"),
                        "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                        "read": notification.get("is_read", False)
                    })
                
                await websocket.send_json({
                    "type": "initial_notifications",
                    "notifications": formatted_notifications,
                    "count": len(formatted_notifications)
                })
        except Exception as e_notif:
            logger.error(f"Error getting initial notifications for {username_for_log}: {e_notif}")
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logger.warning(f"Notify WS: Failed to subscribe {username_for_log}")
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "status": "warning",
                    "message": "Subscription failed, notifications may be delayed"
                })
        
        # Start ping task with longer interval for stability
        ping_task = asyncio.create_task(send_ping(websocket))
        logger.info(f"Notify WS: Setup complete for {username_for_log}")

        # Main message loop with improved error handling
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while websocket.client_state == WebSocketState.CONNECTED and consecutive_errors < max_consecutive_errors:
            try:
                receive_timeout = float(config.get("websocket_receive_timeout", 60.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Reset error counter on successful message
                consecutive_errors = 0
                
                if message.get("type") == "pong":
                    logger.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue
                    
                elif message.get("type") == "mark_read":
                    await handle_mark_read_message(message, user_id_str, username_for_log, websocket)
                
                elif message.get("type") == "acknowledge_fire_alert":
                    alert_id = message.get("alert_id")
                    logger.info(f"Fire alert {alert_id} acknowledged by {username_for_log}")
                            
                else:
                    logger.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

            except asyncio.TimeoutError:
                # Send keepalive on timeout
                if websocket.client_state == WebSocketState.CONNECTED:
                    try:
                        await websocket.send_json({
                            "type": "keepalive",
                            "server_time": datetime.now(timezone.utc).timestamp()
                        })
                    except Exception:
                        break
                continue
                
            except WebSocketDisconnect:
                logger.info(f"Notify WS: Client {username_for_log} disconnected normally")
                websocket_closed = True
                break
                
            except Exception as e_recv:
                consecutive_errors += 1
                logger.error(f"Notify WS: Error receiving from {username_for_log} (#{consecutive_errors}): {e_recv}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors for {username_for_log}, closing connection")
                    break
                    
                await asyncio.sleep(1.0)
    
    except WebSocketDisconnect:
        logger.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
        websocket_closed = True
    except Exception as e_outer:
        logger.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error."})
                websocket_closed = True
                await websocket.close(1011)
            except Exception:
                pass
    finally:
        # Cleanup with improved error handling
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await asyncio.wait_for(ping_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        if user_id_str:
            try:
                await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
            except Exception as e_unsub:
                logger.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        if not websocket_closed:
            await _safe_close_websocket(websocket, username_for_log)


# ==================== HTTP Notification Endpoints ====================

@router.get("/notify")
async def get_http_notifications(
    since: Optional[float] = Query(None, description="Timestamp to get notifications from"),
    limit: int = Query(50, ge=1, le=200),
    include_read: bool = Query(True),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get notifications via HTTP."""
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    
    try:
        from app.services.notification_service import notification_service
        
        # Parse workspace_id if provided
        ws_id = UUID(workspace_id) if workspace_id else None
        
        # Get notifications
        notifications_db = await notification_service.get_user_notifications(
            UUID(user_id_str),
            workspace_id=ws_id,
            unread_only=not include_read,
            limit=limit
        )
        
        # Filter by timestamp if provided
        if since:
            since_dt = datetime.fromtimestamp(since, tz=timezone.utc)
            notifications_db = [
                n for n in notifications_db 
                if n.get("timestamp") and n["timestamp"] > since_dt
            ]
        
        # Format notifications
        formatted_notifications = []
        for notification in notifications_db:
            formatted_notifications.append({
                "id": str(notification.get("notification_id")),
                "user_id": str(notification.get("user_id")),
                "workspace_id": str(notification.get("workspace_id")),
                "stream_id": str(notification.get("stream_id")) if notification.get("stream_id") else None,
                "camera_name": notification.get("camera_name"),
                "status": notification.get("status"),
                "message": notification.get("message"),
                "timestamp": notification.get("timestamp").timestamp() if notification.get("timestamp") else None,
                "read": notification.get("is_read", False)
            })
        
        return {
            "status": "success",
            "count": len(formatted_notifications),
            "notifications": formatted_notifications,
            "server_time": datetime.now(timezone.utc).timestamp()
        }
        
    except Exception as e:
        logger.error(f"Error getting HTTP notifications for user {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}")


@router.post("/notify/{notification_id_str}/read")
async def mark_notification_read_endpoint(
    notification_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Mark a notification as read."""
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    
    try:
        notification_id_uuid = UUID(notification_id_str)
        
        from app.services.notification_service import notification_service
        success = await notification_service.mark_notification_as_read(notification_id_uuid)
        
        if success:
            return {"status": "success", "message": "Notification marked as read"}
        else:
            # Check if notification exists
            notification = await notification_service.get_notification_by_id(notification_id_uuid)
            if not notification:
                raise HTTPException(status_code=404, detail="Notification not found or not yours.")
            return {"status": "info", "message": "Notification was already marked as read."}

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid notification ID format.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking notification {notification_id_str} as read for {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to mark notification as read: {str(e)}")


# ==================== Diagnostics & Debug Endpoints ====================

@router.get("/debug/shared_streams")
async def debug_shared_streams(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Debug endpoint to check shared stream status."""
    # Check if user is admin
    if current_user_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        stats = await stream_manager.get_video_sharing_stats()
        return {"status": "success", "data": stats}
    except Exception as e:
        logger.error(f"Error getting shared stream stats: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


@router.get("/debug/stream/{stream_id}")
async def debug_stream_status(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get detailed debug information for a specific stream."""
    # Check if user is admin
    if current_user_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Validate stream_id format
        try:
            UUID(stream_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid stream_id format")
        
        # Get diagnostic info
        status = await stream_manager.diagnose_stream_activation_failure(stream_id)
        return {"status": "success", "data": status}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in debug_stream_status for stream {stream_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get stream status: {str(e)}")


@router.post("/debug/force_restart_shared/{source_path}")
async def debug_force_restart_shared_stream(
    source_path: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Force restart a shared stream for debugging."""
    # Check if user is admin
    if current_user_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        import urllib.parse
        decoded_source = urllib.parse.unquote(source_path)
        from app.services.shared_stream_service import video_file_manager
        
        result = await video_file_manager.force_restart_shared_stream(decoded_source)
        return {"status": "success" if result else "failed", "source": decoded_source}
    except Exception as e:
        logger.error(f"Error restarting shared stream: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


# ==================== Statistics Endpoints ====================

@router.get("/stats/workspace/{workspace_id}")
async def get_workspace_stream_stats(
    workspace_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get streaming statistics for a workspace."""
    try:
        user_id_str = str(current_user_data["user_id"])
        workspace_id_uuid = UUID(workspace_id)
        
        # Check workspace access
        await check_workspace_access(
            db_manager,
            UUID(user_id_str),
            workspace_id_uuid,
        )
        
        # Get statistics
        from app.services.video_stream_service import video_stream_service
        summary = await video_stream_service.get_workspace_stream_summary(workspace_id_uuid)
        
        # Get active streams info
        active_streams = []
        async with stream_manager._lock:
            for stream_id_str, stream_info in stream_manager.active_streams.items():
                if str(stream_info.get('workspace_id')) == workspace_id:
                    active_streams.append({
                        "stream_id": stream_id_str,
                        "camera_name": stream_info.get('camera_name'),
                        "status": stream_info.get('status'),
                        "start_time": stream_info.get('start_time') if stream_info.get('start_time') else None,
                        "last_frame_time": stream_info.get('last_frame_time') if stream_info.get('last_frame_time') else None
                    })

        content = {
            "status": "success",
            "workspace_id": workspace_id,
            "summary": summary,
            "active_streams": active_streams,
            "active_count": len(active_streams)
        }
        
        return JSONResponse(content=jsonable_encoder(content))
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workspace ID format")
    except Exception as e:
        logger.error(f"Error getting workspace stream stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/global")
async def get_global_stream_stats(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get global streaming statistics (admin only)."""
    if current_user_data.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Get active streams count
        active_count = 0
        workspace_stats = {}
        
        async with stream_manager._lock:
            active_count = len(stream_manager.active_streams)
            
            for stream_id_str, stream_info in stream_manager.active_streams.items():
                ws_id = str(stream_info.get('workspace_id'))
                if ws_id not in workspace_stats:
                    workspace_stats[ws_id] = 0
                workspace_stats[ws_id] += 1
        
        # Get video sharing stats
        sharing_stats = await stream_manager.get_video_sharing_stats()
        
        content = {
            "status": "success",
            "total_active_streams": active_count,
            "workspaces_with_active_streams": len(workspace_stats),
            "workspace_breakdown": workspace_stats,
            "video_sharing": sharing_stats,
            "timestamp": datetime.now(timezone.utc)
        }

        return JSONResponse(content=jsonable_encoder(content))
        
    except Exception as e:
        logger.error(f"Error getting global stream stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")