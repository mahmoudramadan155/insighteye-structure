# app/routes/stream_router.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Response, Depends, Query
from fastapi.responses import JSONResponse
import time
import logging
import asyncio
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
from app.utils import frame_to_base64, check_workspace_access
from app.config.settings import config
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.services.stream_service import stream_manager
from app.utils import safe_close_websocket, send_ping, handle_mark_read_message, send_ping_with_stability_check
from starlette.websockets import WebSocketState
import concurrent.futures
import os

logger = logging.getLogger(__name__) 
router = APIRouter(prefix="/stream", tags=["stream"]) 

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 2 + 4))

@router.post("/start_stream/{stream_id_str}")
async def start_workspace_stream_endpoint( # Renamed to match stream_one.py, path changed
    stream_id_str: str, 
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    try:
        requester_user_id = str(current_user_data["user_id"])
        result = await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id)
        return JSONResponse(content={"status": "success", "details": result})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Error in /start_stream/{stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.post("/stop_stream/{stream_id_str}") # Path changed
async def stop_workspace_stream_endpoint( # Renamed
    stream_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        stream_id_uuid = UUID(stream_id_str)

        stream_info_db = await db_manager.execute_query(
            "SELECT workspace_id, name, user_id FROM video_stream WHERE stream_id = $1",
            (stream_id_uuid,), fetch_one=True
        )

        if not stream_info_db: raise HTTPException(status_code=404, detail="Stream not found.")
        
        s_workspace_id, s_name, s_owner_id = stream_info_db['workspace_id'], stream_info_db['name'], stream_info_db['user_id']
        
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            s_workspace_id,
        )

        updated_rows = await db_manager.execute_query(
            "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
            (datetime.now(timezone.utc), stream_id_uuid), return_rowcount=True
        )
        
        if updated_rows and updated_rows > 0:
             await stream_manager.add_notification(str(s_owner_id), str(s_workspace_id), stream_id_str, s_name, "inactive", f"Camera '{s_name}' stopped by {requester_username}.")
             return JSONResponse(content={"status": "success", "message": f"Stream '{s_name}' stop request processed."}) # Match stream_one
        else: 
            current_status_db = await db_manager.execute_query("SELECT status, is_streaming FROM video_stream WHERE stream_id = $1", (stream_id_uuid,), fetch_one=True)
            if current_status_db and not current_status_db.get('is_streaming', True):
                return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' was already stopped."})
            return JSONResponse(content={"status": "info", "message": f"Stream '{s_name}' could not be stopped or was not found active."}) # Match stream_one

    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except ValueError: # For UUID conversion error
        return JSONResponse(status_code=400, content={"status":"error", "message": "Invalid stream ID format."})
    except Exception as e:
        logging.error(f"Error stopping stream {stream_id_str}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.websocket("/stream")
async def websocket_workspace_stream(websocket: WebSocket):
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
        stream_details_db = await db_manager.execute_query(stream_details_query, (stream_id_uuid,), fetch_one=True)
        
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
        
        # Extract location info for response
        location_data = {
            "location": stream_details_db.get('location'),
            "area": stream_details_db.get('area'),
            "building": stream_details_db.get('building'),
            "zone": stream_details_db.get('zone'),
            "floor_level": stream_details_db.get('floor_level')
        }

        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str),
            s_workspace_id,
        )
        
        stream_is_active_in_manager = False
        async with stream_manager._lock:
            current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
            if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                stream_is_active_in_manager = True
        
        if not s_is_streaming_db or not stream_is_active_in_manager:
            logging.info(f"WS: Stream {stream_id_str} not active (DB: {s_is_streaming_db}, Mgr: {stream_is_active_in_manager}, Status: {s_status_db}). Attempting start by {requester_user_id_str}.")
            
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']:
                await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."})
                websocket_closed = True
                await websocket.close(1008)
                return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str)
                # Wait for stream_manager to pick it up
                for _ in range(config.get("stream_ws_start_wait_attempts", 10)): 
                    async with stream_manager._lock:
                         current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True
                        break
                    await asyncio.sleep(1.0)
                if not stream_is_active_in_manager:
                    db_state_after_start = await db_manager.execute_query(
                        "SELECT status, is_streaming from video_stream where stream_id = $1", 
                        (stream_id_uuid,), fetch_one=True
                    )
                    logger.warning(f"Stream {stream_id_str} failed to become active in StreamManager for WS. DB state: {db_state_after_start}")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize. Check server logs."})
                    websocket_closed = True
                    await websocket.close(1011)
                    return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"})
                websocket_closed = True
                await websocket.close(1011)
                return

        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."})
            websocket_closed = True
            await websocket.close(1011)
            return

        # Check connection before sending confirmation
        if websocket.client_state != WebSocketState.CONNECTED:
            logging.warning(f"WebSocket disconnected before sending confirmation for {stream_id_str}")
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
                    await websocket.send_json({"status":"info", "message":"Stream ended or became inactive."})
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
                        logging.debug(f"Frame send failed: WebSocket already closing for stream {stream_id_str}")
                        break
                    else:
                        raise
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
        websocket_closed = True
    except asyncio.CancelledError:
        logging.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled (User: {user_id_for_log or 'unknown'}).")
    except ValueError as ve: 
        logging.warning(f"WS stream error (User: {user_id_for_log or 'unknown'}, Stream: {stream_id_str or 'unknown'}): Invalid ID format - {ve}")
        if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid stream ID format."})
                websocket_closed = True
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"WS stream error ({stream_id_str or 'unknown'}, User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
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
        
        # Close websocket safely only if not already closed
        if not websocket_closed:
            await safe_close_websocket(websocket, user_id_for_log)

@router.websocket("/notify")
async def websocket_notify(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    websocket_closed = False
    connection_start_time = time.time()

    try:
        await websocket.accept()
        
        # ADD connection delay to prevent rapid reconnections
        await asyncio.sleep(0.5)
        
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

        # ADD connection stability check
        if websocket.client_state != WebSocketState.CONNECTED:
            logging.warning(f"WebSocket disconnected during authentication for {username_for_log}")
            return
        
        # ADD connection rate limiting
        connection_duration = time.time() - connection_start_time
        if connection_duration < 2.0:  # If connection is too fast, add delay
            await asyncio.sleep(2.0 - connection_duration)

        logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({
            "status": "connected", 
            "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get initial notifications with error handling
        try:
            initial_notifications = await stream_manager.get_notifications(
                user_id_str, workspace_id_filter=None, include_read=False, limit=20
            )
            
            # Send initial notifications if any and still connected
            if initial_notifications and websocket.client_state == WebSocketState.CONNECTED:
                formatted_notifications = []
                for notification in initial_notifications:
                    formatted_notifications.append({
                        "id": notification.get("id"),
                        "user_id": notification.get("user_id"),
                        "workspace_id": notification.get("workspace_id"),
                        "stream_id": notification.get("stream_id"),
                        "camera_name": notification.get("camera_name"),
                        "status": notification.get("status"),
                        "message": notification.get("message"),
                        "timestamp": notification.get("timestamp"),
                        "read": notification.get("read", False)
                    })
                
                await websocket.send_json({
                    "type": "initial_notifications",
                    "notifications": formatted_notifications,
                    "count": len(formatted_notifications)
                })
        except Exception as e_notif:
            logging.error(f"Error getting initial notifications for {username_for_log}: {e_notif}")
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "status": "warning", 
                    "message": "Subscription failed, notifications may be delayed"
                })
        
        # ADD ping with longer interval for stability
        ping_task = asyncio.create_task(send_ping_with_stability_check(websocket, username_for_log))
        logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

        # Main message loop with improved error handling
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while websocket.client_state == WebSocketState.CONNECTED and consecutive_errors < max_consecutive_errors:
            try:
                receive_timeout = float(config.get("websocket_receive_timeout", 60.0))  # Increased timeout
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Reset error counter on successful message
                consecutive_errors = 0
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue
                    
                elif message.get("type") == "mark_read":
                    await handle_mark_read_message(message, user_id_str, username_for_log, websocket)
                
                elif message.get("type") == "acknowledge_fire_alert":
                    alert_id = message.get("alert_id")
                    logging.info(f"Fire alert {alert_id} acknowledged by {username_for_log}")
                            
                else:
                    logging.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

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
                logging.info(f"Notify WS: Client {username_for_log} disconnected normally")
                websocket_closed = True
                break
                
            except Exception as e_recv:
                consecutive_errors += 1
                logging.error(f"Notify WS: Error receiving from {username_for_log} (#{consecutive_errors}): {e_recv}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logging.error(f"Too many consecutive errors for {username_for_log}, closing connection")
                    break
                    
                await asyncio.sleep(1.0)  # Brief pause before retrying
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
        websocket_closed = True
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
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
                logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        if not websocket_closed:
            await safe_close_websocket(websocket, username_for_log)

@router.get("/notify")
async def get_http_notifications(
    since: Optional[float] = Query(None, description="Timestamp (seconds since epoch) to get notifications from"),
    limit: int = Query(50, ge=1, le=200),
    include_read: bool = Query(True),
    workspace_id: Optional[str] = Query(None, description="Filter by specific workspace ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    try:
        # stream_one.py uses workspace_id directly.
        notifications = await stream_manager.get_notifications(user_id_str, workspace_id, since, limit, include_read)
        return { # Match stream_one.py payload
            "status": "success", "count": len(notifications),
            "notifications": notifications, "server_time": datetime.now(timezone.utc).timestamp()
        }
    except Exception as e:
        logging.error(f"Error getting HTTP notifications for user {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get notifications: {str(e)}") # Match stream_one

@router.post("/notify/{notification_id_str}/read")
async def mark_notification_read_endpoint(
    notification_id_str: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    user_id_str = str(current_user_data["user_id"])
    username_for_log = current_user_data["username"]
    try:
        notification_id_uuid = UUID(notification_id_str)
        updated_rows = await db_manager.execute_query(
            "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3",
            (datetime.now(timezone.utc), notification_id_uuid, UUID(user_id_str)), return_rowcount=True
        )
        
        if updated_rows and updated_rows > 0:
            return {"status": "success", "message": "Notification marked as read"}
        else: # Check if it exists or was already read (match stream_one.py logic)
            exists_res = await db_manager.execute_query("SELECT is_read FROM notifications WHERE notification_id = $1 AND user_id = $2", (notification_id_uuid, UUID(user_id_str)), fetch_one=True)
            if not exists_res: raise HTTPException(status_code=404, detail="Notification not found or not yours.")
            # if exists_res.get("is_read"): return {"status": "info", "message": "Notification was already marked as read."} # This is covered by updated_rows = 0
            return {"status": "info", "message": "Notification was already marked as read or no change made."} # Match stream_one.py

    except ValueError: raise HTTPException(status_code=400, detail="Invalid notification ID format.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error marking notification {notification_id_str} as read for {username_for_log}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to mark notification as read: {str(e)}")

@router.get("/debug/fire-cooldown/{stream_id}")
async def debug_fire_cooldown_status(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Debug endpoint to check fire notification cooldown status"""
    try:
        status = await stream_manager.get_fire_notification_cooldown_status(stream_id)
        return JSONResponse(content={
            "status": "success",
            "cooldown_info": status,
            "current_time": time.time(),
            "server_time": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting fire cooldown status for {stream_id}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@router.get("/debug/fire-state/{stream_id}")
async def debug_fire_state(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Debug endpoint to check persistent fire state"""
    try:
        stream_uuid = UUID(stream_id)
        persistent_state = await stream_manager.get_persistent_fire_state(stream_uuid)
        
        cooldown_info = {}
        if persistent_state['last_notification_time']:
            time_since_last = (datetime.now(ZoneInfo("Africa/Cairo")) - persistent_state['last_notification_time']).total_seconds()
            cooldown_info = {
                'time_since_last_notification': time_since_last,
                'cooldown_remaining': max(0, stream_manager.fire_cooldown_duration - time_since_last),
                'can_notify': time_since_last >= stream_manager.fire_cooldown_duration
            }
        
        return {
            "status": "success",
            "persistent_state": {
                "fire_status": persistent_state['fire_status'],
                "last_detection_time": persistent_state['last_detection_time'].isoformat() if persistent_state['last_detection_time'] else None,
                "last_notification_time": persistent_state['last_notification_time'].isoformat() if persistent_state['last_notification_time'] else None
            },
            "cooldown_info": cooldown_info,
            "server_time": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/fire-test/{stream_id}")
async def test_fire_cooldown_system(
    stream_id: str,
    action: str = Query("status", description="Action: status, reset, force_notify"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Test endpoint for fire cooldown system"""
    try:
        stream_uuid = UUID(stream_id)
        current_time = datetime.now(timezone.utc)
        
        if action == "status":
            # Get comprehensive status
            persistent_state = await stream_manager.get_persistent_fire_state(stream_uuid)
            cooldown_info = await stream_manager.get_fire_notification_cooldown_status(stream_id)
            
            in_memory_state = stream_manager.fire_detection_states.get(stream_id, "unknown")
            
            return {
                "status": "success",
                "stream_id": stream_id,
                "current_time": current_time.isoformat(),
                "persistent_state": {
                    "fire_status": persistent_state['fire_status'],
                    "last_detection_time": persistent_state['last_detection_time'].isoformat() if persistent_state['last_detection_time'] else None,
                    "last_notification_time": persistent_state['last_notification_time'].isoformat() if persistent_state['last_notification_time'] else None
                },
                "in_memory_state": in_memory_state,
                "cooldown_info": cooldown_info,
                "next_notification_available": (
                    persistent_state['last_notification_time'] + timedelta(seconds=stream_manager.fire_cooldown_duration)
                ).isoformat() if persistent_state['last_notification_time'] else "immediately"
            }
        
        elif action == "reset":
            # Reset cooldown for testing
            stream_manager.fire_notification_cooldowns.pop(stream_id, None)
            await stream_manager.update_persistent_fire_state(stream_uuid, "no detection")
            return {"status": "success", "message": "Fire state and cooldown reset"}
        
        elif action == "force_notify":
            # Force a test notification (bypasses cooldown)
            await stream_manager.add_notification(
                str(current_user_data["user_id"]), 
                "test-workspace", 
                stream_id,
                "Test Camera", 
                "fire_alert", 
                "🔥 TEST FIRE NOTIFICATION - This is a test"
            )
            return {"status": "success", "message": "Test notification sent"}
        
        else:
            return {"status": "error", "message": "Invalid action. Use: status, reset, or force_notify"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/stream-diagnosis/{stream_id}")
async def diagnose_stream_issues(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Diagnose stream activation and processing issues"""
    try:
        diagnosis = await stream_manager.diagnose_stream_activation_failure(stream_id)
        return {"status": "success", "diagnosis": diagnosis}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/people-cooldown/{stream_id}")
async def debug_people_count_cooldown_status(
    stream_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Debug endpoint to check people count notification cooldown status"""
    try:
        current_time = time.time()
        last_notification_time = stream_manager.people_count_notification_cooldowns.get(stream_id, 0)
        
        if last_notification_time == 0:
            status = {
                "stream_id": stream_id,
                "can_notify": True,
                "last_notification": None,
                "cooldown_remaining": 0
            }
        else:
            time_since_last = current_time - last_notification_time
            can_notify = time_since_last >= stream_manager.people_count_cooldown_duration
            cooldown_remaining = max(0, stream_manager.people_count_cooldown_duration - time_since_last)
            
            status = {
                "stream_id": stream_id,
                "can_notify": can_notify,
                "last_notification": datetime.fromtimestamp(last_notification_time, tz=timezone.utc).isoformat(),
                "cooldown_remaining": cooldown_remaining,
                "cooldown_remaining_minutes": cooldown_remaining / 60.0
            }
        
        return JSONResponse(content={
            "status": "success",
            "cooldown_info": status,
            "current_time": current_time,
            "server_time": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting people count cooldown status for {stream_id}: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# @router.websocket("/notify")
# async def websocket_notify(websocket: WebSocket):
#     user_id_str: Optional[str] = None
#     username_for_log: Optional[str] = None
#     ping_task: Optional[asyncio.Task] = None
#     websocket_closed = False  # Track if we've already closed the connection

#     try:
#         await websocket.accept()
#         token = await session_manager.get_token_from_websocket(websocket)
#         if not token:
#             await websocket.send_json({"status": "error", "message": "Authentication token required."})
#             websocket_closed = True
#             await websocket.close(1008)
#             return

#         token_data = await session_manager.verify_token(token, "access")
#         if not token_data or await session_manager.is_token_blacklisted(token):
#             await websocket.send_json({"status": "error", "message": "Invalid or expired token."})
#             websocket_closed = True
#             await websocket.close(1008)
#             return
        
#         user_id_str = token_data.user_id
#         user_db_data = await user_manager.get_user_by_id(UUID(user_id_str))
#         username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

#         logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
#         # Check if still connected before sending
#         if websocket.client_state != WebSocketState.CONNECTED:
#             logging.warning(f"WebSocket disconnected during authentication for {username_for_log}")
#             return
            
#         await websocket.send_json({
#             "status": "connected", 
#             "message": "Connected to notification stream",
#             "server_time": datetime.now(timezone.utc).timestamp()
#         })
        
#         # Get initial notifications
#         initial_notifications = await stream_manager.get_notifications(
#             user_id_str, workspace_id_filter=None, include_read=False, limit=20
#         )
        
#         # Format notifications
#         formatted_notifications = []
#         for notification in initial_notifications:
#             formatted_notifications.append({
#                 "id": notification.get("id"),  # Use "id" not "notification_id"
#                 "user_id": notification.get("user_id"),
#                 "workspace_id": notification.get("workspace_id"),
#                 "stream_id": notification.get("stream_id"),
#                 "camera_name": notification.get("camera_name"),
#                 "status": notification.get("status"),
#                 "message": notification.get("message"),
#                 "timestamp": notification.get("timestamp"),  # Already in correct format
#                 "read": notification.get("read", False)
#             })
        
#         # Send initial notifications if any and still connected
#         if formatted_notifications and websocket.client_state == WebSocketState.CONNECTED:
#             await websocket.send_json(formatted_notifications)
        
#         # Subscribe to notifications
#         subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
#         if not subscription_success:
#             logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
#             if websocket.client_state == WebSocketState.CONNECTED:
#                 await websocket.send_json({
#                     "status": "warning", 
#                     "message": "Subscription failed, notifications may be delayed"
#                 })
        
#         ping_task = asyncio.create_task(send_ping(websocket))
#         logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

#         # Main message loop
#         while websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
#                 message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
#                 if message.get("type") == "pong": 
#                     logging.debug(f"Notification WS: Pong received from {username_for_log}")
#                     continue
                    
#                 elif message.get("type") == "mark_read":
#                     notif_ids_raw = message.get("notification_ids", [])
#                     if isinstance(notif_ids_raw, list) and user_id_str:
#                         updated_count = 0
#                         valid_notif_ids_to_mark: List[UUID] = []
                        
#                         for nid_str_raw in notif_ids_raw:
#                             try:
#                                 valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
#                             except ValueError:
#                                 logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
#                         if valid_notif_ids_to_mark:
#                             for nid_uuid in valid_notif_ids_to_mark:
#                                 try:
#                                     res = await db_manager.execute_query(
#                                         "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
#                                         (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)), 
#                                         return_rowcount=True
#                                     )
#                                     if res and res > 0:
#                                         updated_count += 1
#                                 except Exception as e_mark:
#                                     logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        
#                         # Send acknowledgment only if still connected
#                         if websocket.client_state == WebSocketState.CONNECTED:
#                             await websocket.send_json({
#                                 "type": "ack_mark_read", 
#                                 "ids": notif_ids_raw, 
#                                 "updated_count": updated_count
#                             })
#                 else:
#                     logging.warning(f"Unknown message type from {username_for_log}: {message.get('type')}")

#             except asyncio.TimeoutError:
#                 continue  # No message from client, normal
#             except WebSocketDisconnect:
#                 logging.info(f"Notify WS: Client {username_for_log} disconnected normally")
#                 websocket_closed = True
#                 break
#             except asyncio.CancelledError:
#                 logging.info(f"Notify WS: Task cancelled for {username_for_log}")
#                 raise
#             except Exception as e_recv:
#                 logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True)
#                 break
    
#     except WebSocketDisconnect:
#         logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
#         websocket_closed = True
#     except asyncio.CancelledError:
#         logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
#     except Exception as e_outer:
#         logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
#         # Try to send error message if connection is still viable
#         if not websocket_closed and websocket.client_state == WebSocketState.CONNECTED:
#             try:
#                 await websocket.send_json({"status": "error", "message": "Internal server error."})
#                 websocket_closed = True
#                 await websocket.close(1011)
#             except Exception:
#                 pass  # Ignore error during error handling
#     finally:
#         # Comprehensive cleanup
#         logging.debug(f"Notify WS: Starting cleanup for {username_for_log or 'unknown'}")

#         # Cancel ping task first
#         if ping_task and not ping_task.done():
#             ping_task.cancel()
#             try:
#                 await asyncio.wait_for(ping_task, timeout=1.0)
#             except (asyncio.CancelledError, asyncio.TimeoutError):
#                 pass
#             except Exception as e_ping:
#                 logging.debug(f"Error cancelling ping task for {username_for_log}: {e_ping}")

#         # Unsubscribe from notifications
#         if user_id_str:
#             try:
#                 await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
#             except Exception as e_unsub:
#                 logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

#         # Close websocket safely only if not already closed
#         if not websocket_closed:
#             await safe_close_websocket(websocket, username_for_log)
        
#         logging.debug(f"Notify WS: Cleanup completed for {username_for_log or 'unknown'}")


# @router.get("/streams/locations/search")
# async def search_streams_by_location(
#     location_type: str = Query(..., description="Type of location filter: location, area, building, zone, floor_level"),
#     q: str = Query(..., description="Search query for the location"),
#     current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
# ):
#     """Search streams by location information"""
#     try:
#         user_id_str = str(current_user_data["user_id"])
#         user_id_obj = UUID(user_id_str)
        
#         # Get user's workspaces
#         user_workspaces_db = await db_manager.execute_query(
#             "SELECT workspace_id FROM workspace_members WHERE user_id = $1", 
#             (user_id_obj,), fetch_all=True
#         )
#         user_workspaces_db = user_workspaces_db or []
#         target_workspace_ids_objs = [row['workspace_id'] for row in user_workspaces_db]
        
#         if not target_workspace_ids_objs:
#             return JSONResponse(content={
#                 "status": "success",
#                 "streams": [],
#                 "total": 0,
#                 "message": "No workspaces found for user"
#             })
        
#         # Build the query based on location_type
#         location_column_map = {
#             "location": "vs.location",
#             "area": "vs.area", 
#             "building": "vs.building",
#             "zone": "vs.zone",
#             "floor_level": "vs.floor_level"
#         }
        
#         if location_type not in location_column_map:
#             return JSONResponse(status_code=400, content={
#                 "status": "error",
#                 "message": f"Invalid location_type. Must be one of: {list(location_column_map.keys())}"
#             })
        
#         location_column = location_column_map[location_type]
        
#         # Create placeholders for workspace IDs
#         workspace_placeholders = ', '.join([f'${i+2}' for i in range(len(target_workspace_ids_objs))])
        
#         # Build the search query
#         search_query = f"""
#             SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
#                    vs.user_id as owner_id, u.username as owner_username,
#                    vs.workspace_id, w.name as workspace_name,
#                    vs.created_at, vs.updated_at,
#                    vs.location, vs.area, vs.building, vs.zone, vs.floor_level, 
#                    vs.latitude, vs.longitude,
#                    vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled
#             FROM video_stream vs
#             JOIN users u ON vs.user_id = u.user_id
#             JOIN workspaces w ON vs.workspace_id = w.workspace_id
#             WHERE vs.workspace_id IN ({workspace_placeholders})
#             AND {location_column} ILIKE $1
#             ORDER BY w.name, vs.name
#         """
        
#         # Execute the query
#         search_pattern = f"%{q}%"
#         query_params = [search_pattern] + target_workspace_ids_objs
        
#         streams_data_db = await db_manager.execute_query(
#             search_query, tuple(query_params), fetch_all=True
#         )
#         streams_data_db = streams_data_db or []
        
#         # Format results
#         formatted_streams = []
#         for s in streams_data_db:
#             formatted_streams.append({
#                 "id": str(s["stream_id"]),  # Make sure this matches your frontend expectation
#                 "stream_id": str(s["stream_id"]),
#                 "name": s["name"],
#                 "path": s["path"],
#                 "type": s["type"],
#                 "status": s["status"],
#                 "is_streaming": s["is_streaming"],
#                 "owner_id": str(s["owner_id"]),
#                 "owner_username": s["owner_username"],
#                 "workspace_id": str(s["workspace_id"]),
#                 "workspace_name": s["workspace_name"],
#                 "created_at": s["created_at"].isoformat() if s["created_at"] else None,
#                 "updated_at": s["updated_at"].isoformat() if s["updated_at"] else None,
#                 "can_control": True,
#                 # Location fields
#                 "location": s["location"],
#                 "area": s["area"],
#                 "building": s["building"],
#                 "zone": s["zone"],
#                 "floor_level": s["floor_level"],
#                 "latitude": float(s["latitude"]) if s["latitude"] else None,
#                 "longitude": float(s["longitude"]) if s["longitude"] else None,
#                 # Alert thresholds
#                 "count_threshold_greater": s["count_threshold_greater"],
#                 "count_threshold_less": s["count_threshold_less"],
#                 "alert_enabled": s["alert_enabled"]
#             })
        
#         return JSONResponse(content={
#             "status": "success", 
#             "streams": formatted_streams,
#             "total": len(formatted_streams),
#             "search_criteria": {
#                 "location_type": location_type,
#                 "query": q
#             }
#         })
        
#     except Exception as e:
#         logging.error(f"Error searching streams by location for user {user_id_str}: {e}", exc_info=True)
#         return JSONResponse(status_code=500, content={
#             "status": "error",
#             "message": "Internal server error during location search"
#         })
