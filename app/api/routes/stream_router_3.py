# app/routers/stream_router.py
import logging
import asyncio
from typing import Optional, List
from uuid import UUID
from datetime import datetime, timezone
import time
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Response, Query, status
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState
import cv2
import numpy as np

from app.utils import safe_close_websocket, send_ping, handle_mark_read_message
from app.services.stream_service_3 import stream_manager
from app.services.session_service import session_manager 
from app.services.workspace_service import workspace_service
from app.schemas import (
    StreamStartRequest,
    StreamStopRequest,
    BatchStreamOperation
)

router = APIRouter(prefix="/streams3", tags=["streams3"]) 
logger = logging.getLogger(__name__)


# ==================== Stream Lifecycle Endpoints ====================

@router.post("/start", status_code=status.HTTP_200_OK)
async def start_stream(
    request: StreamStartRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start a video stream.
    
    Validates workspace access, checks quota limits, and initiates stream processing.
    """
    try:
        user_id = str(current_user["user_id"])
        stream_id = str(request.stream_id)
        
        # Start stream with workspace validation
        result = await stream_manager.start_stream_in_workspace(
            stream_id=stream_id,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' started successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start stream: {str(e)}"
        )


@router.post("/stop", status_code=status.HTTP_200_OK)
async def stop_stream(
    request: StreamStopRequest,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop a video stream.
    
    Only stream owner or workspace admin can stop streams.
    """
    try:
        user_id = str(current_user["user_id"])
        stream_id = str(request.stream_id)
        
        # Stop stream with workspace validation
        result = await stream_manager.stop_stream_in_workspace(
            stream_id=stream_id,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' stopped successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop stream: {str(e)}"
        )


@router.post("/restart/{stream_id}", status_code=status.HTTP_200_OK)
async def restart_stream(
    stream_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Restart a video stream.
    
    Stops and immediately restarts the stream. Useful for recovering from errors.
    """
    try:
        user_id = str(current_user["user_id"])
        stream_uuid = UUID(stream_id)
        
        # Stop stream
        await stream_manager.stop_stream_in_workspace(
            stream_id=stream_uuid,
            requester_user_id=user_id
        )
        
        # Wait briefly
        await asyncio.sleep(1.0)
        
        # Start stream
        result = await stream_manager.start_stream_in_workspace(
            stream_id=stream_uuid,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' restarted successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restarting stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart stream: {str(e)}"
        )


# ==================== Stream Status and Information ====================

@router.get("/list")
async def list_user_streams(
    workspace_id: Optional[str] = Query(None),
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    List all streams accessible to the user.
    
    Optionally filter by workspace.
    """
    try:
        user_id = str(current_user["user_id"])
        ws_id = UUID(workspace_id) if workspace_id else None
        
        # Get streams
        result = await stream_manager.get_workspace_streams_for_user(
            user_id=user_id,
            workspace_id=ws_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": result
            }
        )
        
    except Exception as e:
        logger.error(f"Error listing streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list streams: {str(e)}"
        )


# ==================== Workspace Stream Management ====================

@router.post("/workspace/{workspace_id}/start-all")
async def start_all_workspace_streams(
    workspace_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start all inactive streams in a workspace.
    
    Requires workspace admin role.
    Respects workspace quota limits and will start streams up to available capacity.
    """
    try:
        user_id = str(current_user["user_id"])
        ws_id = UUID(workspace_id)
        
        # Validate admin access
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Check workspace limits first
        limits = await stream_manager.get_workspace_stream_limits(ws_id)
        
        if limits['available_slots'] <= 0:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": False,
                    "message": f"Workspace at capacity ({limits['current_active']}/{limits['total_camera_limit']})",
                    "data": {
                        "started_count": 0,
                        "total_count": 0,
                        "skipped_quota": 0,
                        "failed_streams": [],
                        "limits": limits
                    }
                }
            )
        
        # Get ALL streams from database that are NOT currently streaming
        streams_query = """
            SELECT vs.stream_id, vs.name, vs.is_streaming, vs.status, vs.user_id,
                   u.is_active as user_active, u.is_subscribed, u.role as user_role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1 
                AND vs.is_streaming = FALSE
                AND u.is_active = TRUE
                AND (u.is_subscribed = TRUE OR u.role = 'admin')
            ORDER BY vs.created_at ASC
        """
        db_streams = await stream_manager.db_manager.execute_query(
            streams_query, (ws_id,), fetch_all=True
        )
        
        if not db_streams:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": "No inactive streams to start",
                    "data": {
                        "started_count": 0,
                        "total_count": 0,
                        "skipped_quota": 0,
                        "failed_streams": []
                    }
                }
            )
        
        # Start streams up to available quota
        started_count = 0
        skipped_quota = 0
        failed_streams = []
        available_slots = limits['available_slots']
        
        for stream in db_streams:
            stream_id = stream['stream_id']
            stream_name = stream['name']
            stream_user_id = stream['user_id']
            
            # Check if we've reached workspace capacity
            if started_count >= available_slots:
                skipped_quota += 1
                logger.debug(f"Skipping stream {stream_id} due to workspace quota")
                continue
            
            try:
                # Validate user can start stream (check personal limits)
                can_start, reason = await stream_manager.can_start_stream_in_workspace(
                    ws_id, stream_user_id
                )
                
                if not can_start:
                    logger.warning(f"Cannot start stream {stream_id}: {reason}")
                    failed_streams.append({
                        'stream_id': str(stream_id),
                        'camera_name': stream_name,
                        'error': reason
                    })
                    continue
                
                # Start the stream
                await stream_manager.start_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=UUID(user_id)
                )
                started_count += 1
                logger.info(f"Started stream {stream_id} ({stream_name}) in workspace {workspace_id}")
                
                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Failed to start stream {stream_id}: {e}")
                failed_streams.append({
                    'stream_id': str(stream_id),
                    'camera_name': stream_name,
                    'error': str(e)
                })
        
        # FIXED: Invalidate the cache after starting streams
        # This ensures the next /limits call gets fresh data
        cache_key = f"ws_limits_{ws_id}"
        if hasattr(stream_manager, '_limits_cache') and cache_key in stream_manager._limits_cache:
            del stream_manager._limits_cache[cache_key]
            logger.debug(f"Invalidated limits cache for workspace {workspace_id}")
        
        # Calculate remaining quota after this operation
        remaining_slots = available_slots - started_count
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Started {started_count}/{len(db_streams)} streams",
                "data": {
                    "started_count": started_count,
                    "total_count": len(db_streams),
                    "skipped_quota": skipped_quota,
                    "failed_streams": failed_streams,
                    "limits": {
                        **limits,
                        "remaining_after_operation": remaining_slots
                    }
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start workspace streams: {str(e)}"
        )


@router.post("/workspace/{workspace_id}/stop-all")
async def stop_all_workspace_streams(
    workspace_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop all active streams in a workspace.
    
    Requires workspace admin role.
    """
    try:
        user_id = str(current_user["user_id"])
        ws_id = UUID(workspace_id)
        
        # Validate admin access
        membership = await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id,
            required_role="admin"
        )
        
        # Get ALL streams from database that should be stopped
        streams_query = """
            SELECT stream_id, name, is_streaming, status
            FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE
        """
        db_streams = await stream_manager.db_manager.execute_query(
            streams_query, (ws_id,), fetch_all=True
        )
        
        if not db_streams:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": "No active streams to stop",
                    "data": {
                        "stopped_count": 0,
                        "total_count": 0,
                        "failed_streams": []
                    }
                }
            )
        
        # Stop all streams
        stopped_count = 0
        failed_streams = []
        
        for stream in db_streams:
            stream_id = stream['stream_id']
            stream_name = stream['name']
            
            try:
                await stream_manager.stop_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=UUID(user_id)
                )
                stopped_count += 1
                logger.info(f"Stopped stream {stream_id} ({stream_name}) in workspace {workspace_id}")
                
            except Exception as e:
                logger.error(f"Failed to stop stream {stream_id}: {e}")
                failed_streams.append({
                    'stream_id': str(stream_id),
                    'camera_name': stream_name,
                    'error': str(e)
                })
        
        # FIXED: Invalidate the cache after stopping streams
        # This ensures the next /limits call gets fresh data
        cache_key = f"ws_limits_{ws_id}"
        if hasattr(stream_manager, '_limits_cache') and cache_key in stream_manager._limits_cache:
            del stream_manager._limits_cache[cache_key]
            logger.debug(f"Invalidated limits cache for workspace {workspace_id}")
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stopped {stopped_count}/{len(db_streams)} streams",
                "data": {
                    "stopped_count": stopped_count,
                    "total_count": len(db_streams),
                    "failed_streams": failed_streams
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping workspace streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop workspace streams: {str(e)}"
        )

# ==================== WebSocket Endpoints ====================

@router.websocket("/ws/{stream_id}")
async def stream_websocket(
    websocket: WebSocket,
    stream_id: str
):
    """
    WebSocket endpoint for real-time stream data.
    
    Streams processed video frames with detection overlays.
    """
    await websocket.accept()
    ping_task = None
    
    try:
        # Get authentication from query params or headers
        token = websocket.query_params.get("token")
        if not token:
            await websocket.send_json({
                "type": "error",
                "message": "Authentication token required"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Validate token and get user
        # Note: Implement token validation based on your auth system
        try:
            user_data = await session_manager.verify_token(token)
            user_id = UUID(user_data["user_id"])
        except Exception as e:
            await websocket.send_json({
                "type": "error",
                "message": "Invalid authentication token"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        stream_uuid = UUID(stream_id)
        
        # Validate access
        await stream_manager.validate_workspace_stream_access(
            user_id=user_id,
            stream_id=stream_uuid
        )
        
        # Connect client
        connected = await stream_manager.connect_client_to_stream(stream_id, websocket)
        if not connected:
            await websocket.send_json({
                "type": "error",
                "message": "Stream not active or not found"
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Send connection confirmation
        await websocket.send_json({
            "type": "connected",
            "stream_id": stream_id,
            "timestamp": datetime.now().isoformat()
        })
        
        # Start ping task
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Main streaming loop
        while True:
            # Get latest frame
            async with stream_manager._lock:
                stream_info = stream_manager.active_streams.get(stream_id)
            
            if not stream_info:
                await websocket.send_json({
                    "type": "stream_ended",
                    "message": "Stream is no longer active"
                })
                break
            
            latest_frame = stream_info.get('latest_frame')
            
            if latest_frame is not None:
                # Encode frame to JPEG
                _, buffer = cv2.imencode('.jpg', latest_frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                frame_bytes = buffer.tobytes()
                
                # Send frame
                await websocket.send_bytes(frame_bytes)
            
            # Small delay to control frame rate
            await asyncio.sleep(0.033)  # ~30 FPS
        
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for stream {stream_id}")
    except Exception as e:
        logger.error(f"WebSocket error for stream {stream_id}: {e}", exc_info=True)
        try:
            await websocket.send_json({
                "type": "error",
                "message": f"Stream error: {str(e)}"
            })
        except:
            pass
    finally:
        # Cleanup
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        
        await stream_manager.disconnect_client(stream_id, websocket)
        await safe_close_websocket(websocket)


@router.websocket("/ws/notifications")
async def notifications_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for real-time notifications.
    
    Subscribes to workspace and stream notifications.
    """
    await websocket.accept()
    ping_task = None
    user_id_str = None
    
    try:
        # Get authentication
        token = websocket.query_params.get("token")
        if not token:
            await websocket.send_json({
                "type": "error",
                "message": "Authentication token required"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Validate token
        try:
            user_data = await session_manager.verify_token(token)
            user_id_str = user_data["user_id"]
        except Exception as e:
            await websocket.send_json({
                "type": "error",
                "message": "Invalid authentication token"
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Subscribe to notifications
        subscribed = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        
        if not subscribed:
            await websocket.send_json({
                "type": "error",
                "message": "Failed to subscribe to notifications"
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Start ping task
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Keep connection alive
        while True:
            try:
                # Receive messages (for heartbeat/acknowledgment)
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=60.0
                )
                
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                
            except asyncio.TimeoutError:
                # Check if still connected
                if websocket.client_state != WebSocketState.CONNECTED:
                    break
            
    except WebSocketDisconnect:
        logger.info(f"Notification WebSocket disconnected for user {user_id_str}")
    except Exception as e:
        logger.error(f"Notification WebSocket error: {e}", exc_info=True)
    finally:
        # Cleanup
        if ping_task and not ping_task.done():
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass
        
        if user_id_str:
            await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
        
        await safe_close_websocket(websocket)

# ==================== Batch Operations ====================

@router.post("/batch/start")
async def batch_start_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Start multiple streams in batch.
    
    Returns success/failure status for each stream.
    """
    try:
        user_id = str(current_user["user_id"])
        results = []
        
        for stream_id_str in request.stream_ids:
            try:
                stream_id = UUID(stream_id_str)
                result = await stream_manager.start_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=user_id
                )
                
                results.append({
                    "stream_id": stream_id_str,
                    "success": True,
                    "message": f"Started successfully",
                    "data": result
                })
                
            except Exception as e:
                results.append({
                    "stream_id": stream_id_str,
                    "success": False,
                    "message": "Failed to start",
                    "error": str(e)
                })
        
        successful = sum(1 for r in results if r["success"])
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_requested": len(request.stream_ids),
                "successful": successful,
                "failed": len(request.stream_ids) - successful,
                "results": results
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch start: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch start failed: {str(e)}"
        )


@router.post("/batch/stop")
async def batch_stop_streams(
    request: BatchStreamOperation,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stop multiple streams in batch.
    
    Returns success/failure status for each stream.
    """
    try:
        user_id = str(current_user["user_id"])
        results = []
        
        for stream_id_str in request.stream_ids:
            try:
                stream_id = UUID(stream_id_str)
                result = await stream_manager.stop_stream_in_workspace(
                    stream_id=stream_id,
                    requester_user_id=user_id
                )
                
                results.append({
                    "stream_id": stream_id_str,
                    "success": True,
                    "message": f"Stopped successfully",
                    "data": result
                })
                
            except Exception as e:
                results.append({
                    "stream_id": stream_id_str,
                    "success": False,
                    "message": "Failed to stop",
                    "error": str(e)
                })
        
        successful = sum(1 for r in results if r["success"])
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_requested": len(request.stream_ids),
                "successful": successful,
                "failed": len(request.stream_ids) - successful,
                "results": results
            }
        )
        
    except Exception as e:
        logger.error(f"Error in batch stop: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch stop failed: {str(e)}"
        )
