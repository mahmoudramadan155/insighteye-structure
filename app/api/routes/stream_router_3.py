# app/routers/stream_router.py
import logging
import asyncio
from typing import Optional, List
from uuid import UUID
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, Response, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from starlette.websockets import WebSocketState
import cv2
import numpy as np

from app.services.stream_service_3 import stream_manager, send_ping, _safe_close_websocket
from app.services.session_service import session_manager 
from app.services.workspace_service import workspace_service
from app.services.video_stream_service import video_stream_service
from app.schemas import (
    StreamStartRequest,
    StreamStopRequest,
    StreamStatusResponse,
    WorkspaceStreamAnalytics,
    SystemDiagnostics,
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

@router.get("/status/{stream_id}")#, response_model=StreamStatusResponse
async def get_stream_status(
    stream_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get detailed status of a specific stream.
    
    Returns current state, performance metrics, and error information.
    """
    try:
        user_id = str(current_user["user_id"])
        stream_uuid = UUID(stream_id)
        
        # Validate access
        stream_info, membership_info = await stream_manager.validate_workspace_stream_access(
            user_id=user_id,
            stream_id=stream_uuid
        )
        
        # Get diagnostics
        diagnostics = await stream_manager.get_stream_diagnostics(stream_id)
        
        # Check if stream exists
        if diagnostics.get("status") == "not_active" and stream_id not in stream_manager.active_streams:
            # Stream might be in database but not active
            # Enrich with database info
            diagnostics.update({
                "database_info": {
                    "stream_id": str(stream_info.get("stream_id")),
                    "name": stream_info.get("name"),
                    "status": stream_info.get("status"),
                    "is_streaming": stream_info.get("is_streaming"),
                    "workspace_id": str(stream_info.get("workspace_id")),
                    "created_at": jsonable_encoder(stream_info.get("created_at")) if stream_info.get("created_at") else None
                }
            })
        
        # return diagnostics#StreamStatusResponse(**diagnostics)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": diagnostics
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stream status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stream status: {str(e)}"
        )


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


@router.get("/active")
async def get_active_streams(
    workspace_id: Optional[str] = Query(None),
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get all currently active/streaming streams.
    
    Optionally filter by workspace.
    """
    try:
        user_id = str(current_user["user_id"])
        
        if workspace_id:
            ws_id = UUID(workspace_id)
            # Validate workspace access
            await workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=ws_id
            )
            
            active_streams = await stream_manager.get_workspace_active_streams(ws_id)
        else:
            # Get all workspaces user has access to
            user_workspaces = await workspace_service.get_user_workspaces(user_id)
            active_streams = []
            
            for ws in user_workspaces:
                ws_streams = await stream_manager.get_workspace_active_streams(
                    UUID(ws['workspace_id'])
                )
                active_streams.extend(ws_streams)
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "active_streams": active_streams,
                    "count": len(active_streams)
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting active streams: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active streams: {str(e)}"
        )


# ==================== Workspace Stream Management ====================

@router.get("/workspace/{workspace_id}/analytics", response_model=WorkspaceStreamAnalytics)
async def get_workspace_analytics(
    workspace_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get comprehensive analytics for workspace streams.
    
    Includes performance metrics, error rates, and resource usage.
    """
    try:
        user_id = str(current_user["user_id"])
        ws_id = UUID(workspace_id)
        
        # Validate workspace access
        await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id
        )
        
        # Get analytics
        analytics = await stream_manager.get_workspace_stream_analytics(ws_id)
        
        return WorkspaceStreamAnalytics(**analytics)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workspace analytics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get workspace analytics: {str(e)}"
        )


@router.get("/workspace/{workspace_id}/limits")
async def get_workspace_limits(
    workspace_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get stream limits and quota information for a workspace.
    """
    try:
        user_id = str(current_user["user_id"])
        ws_id = UUID(workspace_id)
        
        # Validate workspace access
        await workspace_service.check_workspace_membership_and_get_role(
            user_id=user_id,
            workspace_id=ws_id
        )
        
        # Get limits
        limits = await stream_manager.get_workspace_stream_limits(ws_id)
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": limits
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting workspace limits: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get workspace limits: {str(e)}"
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
        
        # Get active streams
        active_streams = await stream_manager.get_workspace_active_streams(ws_id)
        
        # Stop all streams
        stopped_count = 0
        failed_streams = []
        
        for stream in active_streams:
            try:
                await stream_manager.stop_stream_in_workspace(
                    stream_id=UUID(stream['stream_id']),
                    requester_user_id=user_id
                )
                stopped_count += 1
            except Exception as e:
                logger.error(f"Failed to stop stream {stream['stream_id']}: {e}")
                failed_streams.append({
                    'stream_id': stream['stream_id'],
                    'camera_name': stream.get('camera_name'),
                    'error': str(e)
                })
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stopped {stopped_count}/{len(active_streams)} streams",
                "data": {
                    "stopped_count": stopped_count,
                    "total_count": len(active_streams),
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

# @router.post("/workspace/{workspace_id}/stop-all")
# async def stop_all_workspace_streams(
#     workspace_id: str,
#     force: bool = Query(False, description="Force stop even if errors occur"),
#     current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
# ):
#     """
#     Stop all active streams in a workspace.
#     """
#     try:
#         user_id = str(current_user["user_id"])
        
#         # Validate UUID format
#         try:
#             ws_id = UUID(workspace_id)
#         except ValueError:
#             raise HTTPException(
#                 status_code=status.HTTP_400_BAD_REQUEST,
#                 detail=f"Invalid workspace ID format: {workspace_id}"
#             )
        
#         # Validate admin access
#         try:
#             membership = await workspace_service.check_workspace_membership_and_get_role(
#                 user_id=user_id,
#                 workspace_id=ws_id,
#                 required_role="admin"
#             )
#         except HTTPException as e:
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail=f"Admin access required: {e.detail}"
#             )
        
#         # Get active streams with lock
#         async with stream_manager._lock:
#             workspace_id_str = str(ws_id)
#             stream_ids_in_workspace = list(stream_manager.workspace_streams.get(workspace_id_str, set()))
        
#         if not stream_ids_in_workspace:
#             return JSONResponse(
#                 status_code=status.HTTP_200_OK,
#                 content={
#                     "success": True,
#                     "message": "No active streams in workspace",
#                     "data": {
#                         "stopped_count": 0,
#                         "total_count": 0,
#                         "failed_streams": []
#                     }
#                 }
#             )
        
#         # Get stream details
#         active_streams = []
#         for stream_id_str in stream_ids_in_workspace:
#             try:
#                 stream_uuid = UUID(stream_id_str)
#                 stream_info = await video_stream_service.get_video_stream_by_id(stream_uuid)
#                 if stream_info:
#                     active_streams.append({
#                         'stream_id': stream_id_str,
#                         'stream_uuid': stream_uuid,
#                         'camera_name': stream_info.get('name', 'Unknown'),
#                         'owner_id': stream_info.get('user_id')
#                     })
#             except Exception as e:
#                 logger.error(f"Error getting info for stream {stream_id_str}: {e}")
#                 if not force:
#                     raise HTTPException(
#                         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#                         detail=f"Failed to get stream info: {str(e)}"
#                     )
        
#         if not active_streams:
#             return JSONResponse(
#                 status_code=status.HTTP_200_OK,
#                 content={
#                     "success": True,
#                     "message": "No valid streams found to stop",
#                     "data": {
#                         "stopped_count": 0,
#                         "total_count": 0,
#                         "failed_streams": []
#                     }
#                 }
#             )
        
#         # Stop all streams concurrently with timeout
#         stopped_count = 0
#         failed_streams = []
        
#         async def stop_single_stream(stream_data):
#             try:
#                 await asyncio.wait_for(
#                     stream_manager.stop_stream_in_workspace(
#                         stream_id=stream_data['stream_uuid'],
#                         requester_user_id=user_id
#                     ),
#                     timeout=10.0  # 10 second timeout per stream
#                 )
#                 return {"success": True, "stream_data": stream_data}
#             except asyncio.TimeoutError:
#                 return {
#                     "success": False,
#                     "stream_data": stream_data,
#                     "error": "Timeout stopping stream (>10s)"
#                 }
#             except Exception as e:
#                 return {
#                     "success": False,
#                     "stream_data": stream_data,
#                     "error": str(e)
#                 }
        
#         # Stop streams in batches to avoid overwhelming the system
#         batch_size = 5
#         for i in range(0, len(active_streams), batch_size):
#             batch = active_streams[i:i + batch_size]
            
#             # Create tasks for this batch
#             tasks = [stop_single_stream(stream_data) for stream_data in batch]
            
#             # Wait for batch to complete
#             results = await asyncio.gather(*tasks, return_exceptions=True)
            
#             # Process results
#             for result in results:
#                 if isinstance(result, Exception):
#                     logger.error(f"Exception in batch stop: {result}")
#                     failed_streams.append({
#                         'stream_id': 'unknown',
#                         'camera_name': 'unknown',
#                         'error': str(result)
#                     })
#                 elif isinstance(result, dict):
#                     if result.get("success"):
#                         stopped_count += 1
#                     else:
#                         stream_data = result.get("stream_data", {})
#                         failed_streams.append({
#                             'stream_id': stream_data.get('stream_id', 'unknown'),
#                             'camera_name': stream_data.get('camera_name', 'unknown'),
#                             'error': result.get("error", "Unknown error")
#                         })
            
#             # Small delay between batches
#             if i + batch_size < len(active_streams):
#                 await asyncio.sleep(0.5)
        
#         # Determine overall success
#         total_count = len(active_streams)
#         all_stopped = stopped_count == total_count
        
#         response_data = {
#             "stopped_count": stopped_count,
#             "total_count": total_count,
#             "failed_count": len(failed_streams),
#             "failed_streams": failed_streams
#         }
        
#         if all_stopped:
#             message = f"Successfully stopped all {stopped_count} streams"
#             response_status = status.HTTP_200_OK
#         elif stopped_count > 0:
#             message = f"Partially successful: stopped {stopped_count}/{total_count} streams"
#             response_status = status.HTTP_207_MULTI_STATUS if not force else status.HTTP_200_OK
#         else:
#             message = "Failed to stop any streams"
#             response_status = status.HTTP_500_INTERNAL_SERVER_ERROR if not force else status.HTTP_200_OK
        
#         return JSONResponse(
#             status_code=response_status,
#             content={
#                 "success": all_stopped or (force and stopped_count > 0),
#                 "message": message,
#                 "data": response_data
#             }
#         )
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error stopping workspace streams: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to stop workspace streams: {str(e)}"
#         )

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
        await _safe_close_websocket(websocket)


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
        
        await _safe_close_websocket(websocket)

# @router.websocket("/ws/notifications")
# async def notifications_websocket(websocket: WebSocket):
#     """
#     WebSocket endpoint for real-time notifications.
#     """
#     await websocket.accept()
#     ping_task = None
#     user_id_str = None
    
#     try:
#         # Get authentication
#         token = websocket.query_params.get("token")
#         if not token:
#             await websocket.send_json({
#                 "type": "error",
#                 "message": "Authentication token required"
#             })
#             await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
#             return
        
#         # Validate token
#         try:
#             user_data = await session_manager.verify_token(token)
#             user_id_str = user_data["user_id"]
#         except Exception as e:
#             logger.warning(f"Invalid token for notifications WebSocket: {e}")
#             await websocket.send_json({
#                 "type": "error",
#                 "message": "Invalid authentication token"
#             })
#             await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
#             return
        
#         # Subscribe to notifications
#         subscribed = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        
#         if not subscribed:
#             await websocket.send_json({
#                 "type": "error",
#                 "message": "Failed to subscribe to notifications"
#             })
#             await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
#             return
        
#         # Start ping task
#         ping_task = asyncio.create_task(send_ping(websocket))
        
#         # Keep connection alive
#         while True:
#             try:
#                 # Check connection state
#                 if websocket.client_state != WebSocketState.CONNECTED:
#                     logger.info(f"WebSocket disconnected for user {user_id_str}")
#                     break
                
#                 # Receive messages (for heartbeat/acknowledgment)
#                 try:
#                     message = await asyncio.wait_for(
#                         websocket.receive_json(),
#                         timeout=60.0
#                     )
                    
#                     if message.get("type") == "ping":
#                         await websocket.send_json({
#                             "type": "pong",
#                             "timestamp": datetime.now().timestamp()
#                         })
                    
#                 except asyncio.TimeoutError:
#                     # Check if still connected after timeout
#                     if websocket.client_state != WebSocketState.CONNECTED:
#                         break
#                     continue
                
#             except WebSocketDisconnect:
#                 logger.info(f"WebSocket disconnected for user {user_id_str}")
#                 break
#             except Exception as e:
#                 logger.error(f"Error in notification WebSocket loop: {e}", exc_info=True)
#                 break
            
#     except WebSocketDisconnect:
#         logger.info(f"Notification WebSocket disconnected for user {user_id_str}")
#     except Exception as e:
#         logger.error(f"Notification WebSocket error: {e}", exc_info=True)
#         try:
#             if websocket.client_state == WebSocketState.CONNECTED:
#                 await websocket.send_json({
#                     "type": "error",
#                     "message": f"Server error: {str(e)}"
#                 })
#         except:
#             pass
#     finally:
#         # Cleanup
#         if ping_task and not ping_task.done():
#             ping_task.cancel()
#             try:
#                 await ping_task
#             except asyncio.CancelledError:
#                 pass
        
#         if user_id_str:
#             await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
        
#         await _safe_close_websocket(websocket, username_for_log=user_id_str)

# ==================== System Diagnostics (Admin Only) ====================

@router.get("/diagnostics/system", response_model=SystemDiagnostics)
async def get_system_diagnostics(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get comprehensive system diagnostics.
    
    Admin only. Provides detailed information about all streams and system health.
    """
    try:
        # Check admin role
        if current_user.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required"
            )
        
        # Get diagnostics
        diagnostics = await stream_manager.get_system_diagnostics()
        
        return SystemDiagnostics(**diagnostics)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting system diagnostics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get system diagnostics: {str(e)}"
        )


@router.post("/diagnostics/health-check")
async def trigger_health_check(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Manually trigger a health check on all streams.
    
    Admin only.
    """
    try:
        # Check admin role
        if current_user.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required"
            )
        
        # Run health check
        await stream_manager._check_stream_health()
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": "Health check completed",
                "timestamp": datetime.now().isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running health check: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to run health check: {str(e)}"
        )


@router.get("/diagnostics/metrics")
async def get_system_metrics(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get system performance metrics.
    
    Admin only.
    """
    try:
        # Check admin role
        if current_user.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required"
            )
        
        metrics = {
            "performance": stream_manager.metrics,
            "active_streams": len(stream_manager.active_streams),
            "workspace_count": len(stream_manager.workspace_streams),
            "notification_subscribers": len(stream_manager.notification_subscribers),
            "shared_sources": len(stream_manager._shared_stream_registry),
            "timestamp": datetime.now(timezone.utc)
        }
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=jsonable_encoder({
                "success": True,
                "data": metrics
            })
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting metrics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get metrics: {str(e)}"
        )


# ==================== Stream Snapshot ====================

# @router.get("/snapshot/{stream_id}")
# async def get_stream_snapshot(
#     stream_id: str,
#     current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
# ):
#     """
#     Get a single frame snapshot from an active stream.
    
#     Returns JPEG image.
#     """
#     try:
#         user_id = str(current_user["user_id"])
#         stream_uuid = UUID(stream_id)
        
#         # Validate access
#         await stream_manager.validate_workspace_stream_access(
#             user_id=user_id,
#             stream_id=stream_uuid
#         )
        
#         # Get latest frame
#         async with stream_manager._lock:
#             stream_info = stream_manager.active_streams.get(stream_id)
        
#         if not stream_info:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="Stream not active"
#             )
        
#         latest_frame = stream_info.get('latest_frame')
        
#         if latest_frame is None:
#             raise HTTPException(
#                 status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#                 detail="No frame available yet"
#             )
        
#         # Encode frame to JPEG
#         _, buffer = cv2.imencode('.jpg', latest_frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
#         frame_bytes = buffer.tobytes()
        
#         return StreamingResponse(
#             iter([frame_bytes]),
#             media_type="image/jpeg",
#             headers={
#                 "Cache-Control": "no-cache, no-store, must-revalidate",
#                 "Pragma": "no-cache",
#                 "Expires": "0"
#             }
#         )
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error getting snapshot: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to get snapshot: {str(e)}"
#         )

@router.get("/snapshot/{stream_id}")
async def get_stream_snapshot(
    stream_id: str,
    quality: int = Query(90, ge=1, le=100, description="JPEG quality (1-100)"),
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get a single frame snapshot from an active stream.

    """
    try:
        user_id = str(current_user["user_id"])
        
        # Validate UUID format
        try:
            stream_uuid = UUID(stream_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid stream ID format: {stream_id}"
            )
        
        # Validate access
        try:
            await stream_manager.validate_workspace_stream_access(
                user_id=user_id,
                stream_id=stream_uuid
            )
        except HTTPException as e:
            raise HTTPException(
                status_code=e.status_code,
                detail=f"Access denied: {e.detail}"
            )
        
        # Get latest frame with lock for thread safety
        latest_frame = None
        stream_info = None
        
        async with stream_manager._lock:
            stream_info = stream_manager.active_streams.get(stream_id)
            if stream_info:
                latest_frame = stream_info.get('latest_frame')
                # Make a copy to avoid issues if frame is modified
                if latest_frame is not None:
                    try:
                        latest_frame = latest_frame.copy()
                    except Exception as e:
                        logger.warning(f"Could not copy frame: {e}")
                        # Continue with original frame
        
        # Check if stream exists
        if not stream_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stream {stream_id} is not currently active"
            )
        
        # Check if frame is available
        if latest_frame is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frame available yet. Stream may be starting or experiencing issues."
            )
        
        # Validate frame data
        if not isinstance(latest_frame, np.ndarray):
            logger.error(f"Invalid frame type for stream {stream_id}: {type(latest_frame)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Invalid frame data"
            )
        
        if latest_frame.size == 0:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Frame data is empty"
            )
        
        # Encode frame to JPEG
        try:
            encode_params = [cv2.IMWRITE_JPEG_QUALITY, quality]
            success, buffer = cv2.imencode('.jpg', latest_frame, encode_params)
            
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to encode frame as JPEG"
                )
            
            frame_bytes = buffer.tobytes()
            
        except Exception as e:
            logger.error(f"Error encoding frame for stream {stream_id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to encode frame: {str(e)}"
            )
        
        # Get camera name for filename
        camera_name = stream_info.get('camera_name', 'camera')
        safe_camera_name = "".join(c for c in camera_name if c.isalnum() or c in ('-', '_')).rstrip()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_camera_name}_{timestamp}.jpg"
        
        # Return image with proper headers
        return Response(
            content=frame_bytes,
            media_type="image/jpeg",
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
                "X-Stream-ID": stream_id,
                "X-Timestamp": timestamp
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting snapshot for stream {stream_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get snapshot: {str(e)}"
        )

# ==================== Error Recovery ====================

@router.post("/recover/{stream_id}")
async def recover_stream(
    stream_id: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Attempt to recover a stream in error state.
    
    Performs cleanup and restart.
    """
    try:
        user_id = str(current_user["user_id"])
        stream_uuid = UUID(stream_id)
        
        # Validate access
        stream_info, _ = await stream_manager.validate_workspace_stream_access(
            user_id=user_id,
            stream_id=stream_uuid
        )
        
        # Check current state
        from app.services.stream_service_3 import StreamState
        current_state = stream_manager.stream_states.get(stream_id)
        
        if current_state != StreamState.ERROR:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": f"Stream is not in error state (current: {current_state.value if current_state else 'unknown'})",
                    "action": "none"
                }
            )
        
        # Force stop
        await stream_manager._stop_stream(stream_id, for_restart=False)
        await asyncio.sleep(2.0)
        
        # Restart
        result = await stream_manager.start_stream_in_workspace(
            stream_id=stream_uuid,
            requester_user_id=user_id
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "message": f"Stream '{result['name']}' recovered successfully",
                "data": result
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error recovering stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to recover stream: {str(e)}"
        )


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


# ==================== Shared Stream Management ====================

@router.get("/shared/info")
async def get_shared_streams_info(
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get information about all shared video streams.
    
    Shows which streams are sharing video sources.
    """
    try:
        # Check admin role
        if current_user.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required"
            )
        
        # Get shared stream stats
        stats = stream_manager.video_file_manager.get_all_stats()
        
        shared_streams = []
        total_subscribers = 0
        
        for source, stream_stats in stats.items():
            subscribers = []
            for sub_id, sub_info in stream_stats.get('subscribers', {}).items():
                subscribers.append({
                    "stream_id": sub_id,
                    "frames_received": sub_info.get('frames_received', 0),
                    "last_frame_time": sub_info.get('last_frame_time')
                })
                total_subscribers += 1
            
            shared_streams.append({
                "source": source,
                "subscriber_count": stream_stats.get('subscriber_count', 0),
                "is_running": stream_stats.get('is_running', False),
                "frame_count": stream_stats.get('frame_count', 0),
                "last_frame_time": stream_stats.get('last_frame_time'),
                "reconnect_attempts": stream_stats.get('reconnect_attempts', 0),
                "last_error": stream_stats.get('last_error'),
                "error_count": stream_stats.get('error_count', 0),
                "subscribers": subscribers
            })
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "shared_streams": shared_streams,
                    "total_sources": len(shared_streams),
                    "total_subscribers": total_subscribers
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting shared streams info: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get shared streams info: {str(e)}"
        )


@router.post("/shared/{source_path}/restart")
async def restart_shared_stream(
    source_path: str,
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Force restart a shared video stream.
    
    Admin only. Useful for recovering stuck shared streams.
    """
    try:
        # Check admin role
        if current_user.get("role") != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required"
            )
        
        # Restart shared stream
        success = await stream_manager.video_file_manager.force_restart_shared_stream(
            source_path
        )
        
        if success:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "success": True,
                    "message": f"Shared stream restarted: {source_path}"
                }
            )
        else:
            return JSONResponse(
                status_code=status.HTTP_404_NOT_FOUND,
                content={
                    "success": False,
                    "message": f"Shared stream not found: {source_path}"
                }
            )
        
    except Exception as e:
        logger.error(f"Error restarting shared stream: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to restart shared stream: {str(e)}"
        )


# ==================== Stream Errors ====================

@router.get("/errors")
async def get_stream_errors(
    workspace_id: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get streams with recent errors.
    
    Optionally filter by workspace.
    """
    try:
        user_id = str(current_user["user_id"])
        
        # Get accessible streams
        if workspace_id:
            ws_id = UUID(workspace_id)
            await workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=ws_id
            )
            stream_ids = stream_manager.workspace_streams.get(workspace_id, set())
        else:
            # Get all accessible streams
            user_workspaces = await workspace_service.get_user_workspaces(user_id)
            stream_ids = set()
            for ws in user_workspaces:
                ws_streams = stream_manager.workspace_streams.get(
                    ws['workspace_id'], set()
                )
                stream_ids.update(ws_streams)
        
        # Collect error information
        errors = []
        for stream_id in stream_ids:
            if stream_id in stream_manager.stream_errors:
                error_list = stream_manager.stream_errors[stream_id]
                if error_list:
                    # Get stream info
                    stream_info = await video_stream_service.get_video_stream_by_id(
                        UUID(stream_id)
                    )
                    
                    errors.append({
                        "stream_id": stream_id,
                        "camera_name": stream_info.get('name') if stream_info else 'Unknown',
                        "error_count": len(error_list),
                        "last_error": error_list[-1]['error'] if error_list else None,
                        "last_error_time": error_list[-1]['timestamp'].isoformat() if error_list else None,
                        "recent_errors": [
                            {
                                "timestamp": err['timestamp'].isoformat(),
                                "operation": err['operation'],
                                "error": err['error']
                            }
                            for err in error_list[-limit:]
                        ]
                    })
        
        # Sort by error count
        errors.sort(key=lambda x: x['error_count'], reverse=True)
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "errors": errors[:limit],
                    "total_streams_with_errors": len(errors),
                    "total_error_count": sum(e['error_count'] for e in errors)
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stream errors: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stream errors: {str(e)}"
        )


# ==================== Stream States ====================

@router.get("/states")
async def get_stream_states(
    workspace_id: Optional[str] = Query(None),
    current_user: dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get current states of all streams.
    
    Shows lifecycle state (STARTING, ACTIVE, STOPPING, ERROR, INACTIVE).
    """
    try:
        user_id = str(current_user["user_id"])
        
        # Get accessible streams
        if workspace_id:
            ws_id = UUID(workspace_id)
            await workspace_service.check_workspace_membership_and_get_role(
                user_id=user_id,
                workspace_id=ws_id
            )
            
            # Get streams for workspace
            streams = await video_stream_service.get_workspace_streams(ws_id)
        else:
            # Get all accessible streams
            result = await stream_manager.get_workspace_streams_for_user(user_id)
            streams = result['streams']
        
        # Collect state information
        states = []
        for stream in streams:
            stream_id = stream.get('stream_id')
            if isinstance(stream_id, UUID):
                stream_id = str(stream_id)
            
            from app.services.stream_service_3 import StreamState
            state = stream_manager.stream_states.get(stream_id, StreamState.INACTIVE)
            in_memory = stream_id in stream_manager.active_streams
            
            states.append({
                "stream_id": stream_id,
                "state": state.value if hasattr(state, 'value') else str(state),
                "status": stream.get('status', 'unknown'),
                "is_streaming": stream.get('is_streaming', False),
                "in_memory": in_memory
            })
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True,
                "data": {
                    "states": states,
                    "total": len(states)
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stream states: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stream states: {str(e)}"
        )

@router.get("/health")
async def stream_service_health():
    """
    Check health of stream service.
    
    Useful for monitoring and debugging.
    """
    try:
        active_count = len(stream_manager.active_streams)
        workspace_count = len(stream_manager.workspace_streams)
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=jsonable_encoder({
                "success": True,
                "status": "healthy",
                "data": {
                    "active_streams": active_count,
                    "workspaces": workspace_count,
                    "timestamp": datetime.now()
                }
            })
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "success": False,
                "status": "unhealthy",
                "error": str(e)
            }
        )
    