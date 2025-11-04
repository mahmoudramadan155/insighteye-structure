# app/routes/stream_router2.py
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Response, Depends, Query
from fastapi.responses import JSONResponse
import time
import logging
import asyncio
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Any, Union
from app.utils import check_workspace_access, frame_to_base64, parse_string_or_list, encoded_string 
from app.config.settings import config
from app.services.database import db_manager
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.services.stream_service2 import stream_manager, send_ping, _safe_close_websocket, process_bulk_start_request
import concurrent.futures
import os
from starlette.websockets import WebSocketState
from app.schemas import ThresholdSettings

logger = logging.getLogger(__name__) 
router = APIRouter(prefix="/stream2", tags=["stream2"]) 

# ThreadPoolExecutor for CPU-bound tasks like YOLO and cv2
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) * 2 + 4))

# === Endpoints to match stream_one.py ===

@router.get("/streams") # Path changed
async def get_all_workspace_streams_endpoint( # Renamed
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional, defaults to user's active or all accessible)"), # Match stream_one description
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    try:
        user_id_str = str(current_user_data["user_id"])
        result = await stream_manager.get_workspace_streams(user_id_str, workspace_id)
        return JSONResponse(content={"status": "success", "data": result}) # Match stream_one response structure
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"status": "error", "message": e.detail})
    except Exception as e:
        logging.error(f"Error getting workspace streams: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

# === ADDING MISSING BULK OPERATION ENDPOINTS ===
@router.post("/start_all_streams", summary="Start all eligible streams in a workspace")
async def start_all_streams_in_workspace(
    workspace_id: UUID = Query(..., description="The ID of the workspace for which to start all streams"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    _user_id_val = current_user_data["user_id"]
    if isinstance(_user_id_val, UUID):
        requester_user_id = _user_id_val
    else:
        try:
            requester_user_id = UUID(str(_user_id_val))
        except ValueError:
            logger.error(f"Invalid user_id format encountered in start_all_streams: {_user_id_val}")
            raise HTTPException(status_code=400, detail="Invalid user ID format in token.")
            
    requester_username = current_user_data["username"]

    try:
        workspace_role_data = await check_workspace_access(
            db_manager,
            requester_user_id, 
            workspace_id,
        )
        if workspace_role_data.get("role") not in ["owner", "admin", "member"]:
            raise HTTPException(status_code=403, detail="User does not have permission to start all streams in this workspace.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error during permission check for start_all_streams by {requester_username} in ws {workspace_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error verifying workspace permissions.")

    results = []
    streams_started_count = 0
    streams_processed_count = 0

    try:
        streams_to_consider_query = """
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.workspace_id = $1 AND vs.is_streaming = FALSE;
        """
        streams_to_consider = await db_manager.execute_query(streams_to_consider_query, (workspace_id,), fetch_all=True)
        streams_to_consider = streams_to_consider or []
        
        streams_processed_count = len(streams_to_consider)
        if not streams_to_consider:
            return JSONResponse(content={"status": "info", "message": "No streams available to start...", "workspace_id": str(workspace_id), "streams_processed": 0, "streams_started": 0, "details": []})

        owner_active_streams_count_query = """
            SELECT user_id, COUNT(*) as active_count FROM video_stream
            WHERE workspace_id = $1 AND is_streaming = TRUE GROUP BY user_id;
        """
        active_counts_db = await db_manager.execute_query(owner_active_streams_count_query, (workspace_id,), fetch_all=True)
        active_counts_db = active_counts_db or []
        owner_active_streams_map = {row['user_id']: row['active_count'] for row in active_counts_db}

        for stream_data in streams_to_consider:
            stream_id, stream_name, owner_id = stream_data['stream_id'], stream_data['name'], stream_data['owner_id']
            
            if not stream_data['owner_is_active']:
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream owner is inactive."}); continue
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream owner not subscribed (and not admin)." }); continue

            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get(owner_id, 0)

            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": f"Owner camera limit ({owner_limit}) reached."}); continue

            await db_manager.execute_query(
                "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                (datetime.now(timezone.utc), stream_id)
            )
            streams_started_count += 1
            owner_active_streams_map[owner_id] = current_owner_active_count + 1 
            results.append({"stream_id": str(stream_id), "name": stream_name, "status": "initiated", "message": "Stream start initiated."})

        return JSONResponse(content={"status": "success" if streams_started_count == streams_processed_count else "partial_success", "workspace_id": str(workspace_id), "streams_processed": streams_processed_count, "streams_started": streams_started_count, "details": results})
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error in start_all_streams for ws {workspace_id} by {requester_username}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error starting streams.", "workspace_id": str(workspace_id), "details": results})

@router.post("/stop_all_streams", summary="Stop all running streams in a workspace")
async def stop_all_streams_in_workspace(
    workspace_id: UUID = Query(..., description="The ID of the workspace for which to stop all streams"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    _user_id_val = current_user_data["user_id"]
    if isinstance(_user_id_val, UUID):
        requester_user_id = _user_id_val
    else:
        try:
            requester_user_id = UUID(str(_user_id_val))
        except ValueError:
            logger.error(f"Invalid user_id format encountered in stop_all_streams: {_user_id_val}")
            raise HTTPException(status_code=400, detail="Invalid user ID format in token.")

    requester_username = current_user_data["username"]

    try:
        workspace_role_data = await check_workspace_access(
            db_manager,
            requester_user_id, 
            workspace_id,
        )
        if workspace_role_data.get("role") not in ["owner", "admin", "member"]:
            raise HTTPException(status_code=403, detail="User does not have permission to stop all streams in this workspace.")
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error during permission check for stop_all_streams by {requester_username} in ws {workspace_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error verifying workspace permissions.")

    results = []
    streams_stopped_count = 0
    streams_processed_count = 0
    
    try:
        streams_to_stop_query = """
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id
            FROM video_stream vs WHERE vs.workspace_id = $1 AND vs.is_streaming = TRUE;
        """
        streams_to_stop = await db_manager.execute_query(streams_to_stop_query, (workspace_id,), fetch_all=True)
        streams_to_stop = streams_to_stop or []

        streams_processed_count = len(streams_to_stop)
        if not streams_to_stop:
            return JSONResponse(content={"status": "info", "message": "No streams running to stop.", "workspace_id": str(workspace_id), "streams_processed": 0, "streams_stopped": 0, "details": []})

        for stream_data in streams_to_stop:
            stream_id, stream_name, owner_id = stream_data['stream_id'], stream_data['name'], stream_data['owner_id']
            updated_rows = await db_manager.execute_query(
                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
                (datetime.now(timezone.utc), stream_id), return_rowcount=True
            )
            if updated_rows and updated_rows > 0:
                streams_stopped_count += 1
                results.append({"stream_id": str(stream_id), "name": stream_name, "status": "stopped", "message": "Stream stop request processed."})
                await stream_manager.add_notification(str(owner_id), str(workspace_id), str(stream_id), stream_name, "inactive", f"Camera '{stream_name}' stopped by {requester_username} (bulk action).")
            else:
                 results.append({"stream_id": str(stream_id), "name": stream_name, "status": "skipped", "message": "Stream already stopped or issue."})

        return JSONResponse(content={"status": "success" if streams_stopped_count == streams_processed_count else "partial_success", "workspace_id": str(workspace_id), "streams_processed": streams_processed_count, "streams_stopped": streams_stopped_count, "details": results})
    except HTTPException: raise
    except Exception as e:
        logging.error(f"Error in stop_all_streams for ws {workspace_id} by {requester_username}: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error stopping streams.", "workspace_id": str(workspace_id), "details": results})

@router.post("/start_all_streams_v2")
async def start_all_streams_v2_endpoint( # Name to match stream_one
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        update_query = """
            UPDATE video_stream vs SET is_streaming = TRUE, status = 'processing', updated_at = $1
            FROM workspace_members wm
            WHERE vs.workspace_id = wm.workspace_id AND wm.user_id = $2 AND vs.is_streaming = FALSE
            RETURNING vs.stream_id;
        """ # RETURNING to get count
        now_utc = datetime.now(timezone.utc)
        updated_streams = await db_manager.execute_query(update_query, (now_utc, UUID(requester_user_id_str)), fetch_all=True)
        updated_rows = len(updated_streams) if updated_streams else 0

        if updated_rows == 0:
            return JSONResponse(content={"status": "info", "message": "No inactive streams to start in your accessible workspaces."})
        logging.info(f"User {requester_username} requested to start all v2 streams. {updated_rows} streams marked for starting.")
        return JSONResponse(content={"status": "success", "message": f"{updated_rows} streams marked to start."})
    except Exception as e:
        logging.error(f"Error in /start_all_streams_v2: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.post("/stop_all_streams_v2")
async def stop_all_streams_v2_endpoint( # Name to match stream_one
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id
            FROM video_stream vs JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE
        """
        streams_to_stop = await db_manager.execute_query(query, (UUID(requester_user_id_str),), fetch_all=True)
        streams_to_stop = streams_to_stop or []

        if not streams_to_stop:
            return JSONResponse(content={"status": "info", "message": "No active streams to stop in your accessible workspaces."})

        stream_ids = [s["stream_id"] for s in streams_to_stop] # These are already UUIDs from DB
        placeholders = ','.join([f'${i+2}' for i in range(len(stream_ids))]) # $1 for time, $2... for stream_ids
        
        update_query = f"""
            UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1
            WHERE stream_id IN ({placeholders})
        """
        now_utc = datetime.now(timezone.utc)
        await db_manager.execute_query(update_query, (now_utc, *stream_ids))

        for stream in streams_to_stop:
            await stream_manager.add_notification(str(stream["user_id"]), str(stream["workspace_id"]), str(stream["stream_id"]), stream["name"], "inactive", f"Camera '{stream['name']}' stopped by {requester_username} (v2 bulk action).")
        logging.info(f"User {requester_username} stopped {len(streams_to_stop)} streams (v2).")
        return JSONResponse(content={"status": "success", "message": f"{len(streams_to_stop)} streams stopped."})
    except Exception as e:
        logging.error(f"Error in /stop_all_streams_v2: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

# === NEW: LOCATION-BASED FILTERING ENDPOINT ===
@router.get("/streams/by-location")
async def get_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get streams filtered by location criteria."""
    try:
        user_id_str = str(current_user_data["user_id"])
        
        # Build query with location filters
        base_query = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
                   vs.latitude, vs.longitude, u.username as owner_username,
                   w.name as workspace_name, vs.workspace_id
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
            
        base_query += " ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location, vs.name"
        
        streams = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        streams = streams or []
        
        formatted_streams = [{
            "stream_id": str(s["stream_id"]),
            "name": s["name"],
            "status": s["status"],
            "is_streaming": s["is_streaming"],
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "floor_level": s["floor_level"],
            "latitude": float(s["latitude"]) if s["latitude"] else None,
            "longitude": float(s["longitude"]) if s["longitude"] else None,
            "owner_username": s["owner_username"],
            "workspace_name": s["workspace_name"],
            "workspace_id": str(s["workspace_id"])
        } for s in streams]
        
        return JSONResponse(content={
            "status": "success",
            "streams": formatted_streams,
            "total": len(formatted_streams),
            "filters_applied": {
                "location": location,
                "area": area, 
                "building": building,
                "floor_level": floor_level,
                "zone": zone
            }
        })
        
    except Exception as e:
        logging.error(f"Error getting streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error."})

@router.websocket("/stream1") # Path changed
async def websocket_workspace_stream(websocket: WebSocket): # Name kept from async_stream
    stream_id_str: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    user_id_for_log: Optional[str] = None

    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        stream_id_str = query_params.get("stream_id")
        if not stream_id_str:
            await websocket.send_json({"status": "error", "message": "stream_id is required."}); await websocket.close(1008); return # Payload match

        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return # Payload match

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return # Payload match
        
        requester_user_id_str = token_data.user_id
        user_id_for_log = requester_user_id_str 
        stream_id_uuid = UUID(stream_id_str)

        # UPDATED: Include location data in connection confirmation
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
            await websocket.send_json({"status": "error", "message": "Stream not found."}); await websocket.close(1008); return

        s_workspace_id = stream_details_db['workspace_id']
        s_name = stream_details_db['name']
        s_is_streaming_db = stream_details_db['is_streaming']
        s_status_db = stream_details_db['status']
        s_owner_username = stream_details_db['owner_username']
        
        # NEW: Extract location info for response
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
            # Role check from stream_one.py before attempting start
            if requester_role_info.get("role") not in ['admin', 'member', 'owner']: # owner can also start
                 await websocket.send_json({"status": "error", "message": "Stream is not active. You do not have permission to start it."}); await websocket.close(1008); return

            try:
                await stream_manager.start_stream_in_workspace(stream_id_str, requester_user_id_str) # This marks for processing
                # Wait for stream_manager to pick it up
                for _ in range(config.get("stream_ws_start_wait_attempts", 10)): 
                    async with stream_manager._lock:
                         current_stream_info_manager = stream_manager.active_streams.get(stream_id_str)
                    if current_stream_info_manager and current_stream_info_manager.get('status') == 'active':
                        stream_is_active_in_manager = True; break
                    await asyncio.sleep(1.0)
                if not stream_is_active_in_manager:
                    db_state_after_start = await db_manager.execute_query("SELECT status, is_streaming from video_stream where stream_id = $1", (stream_id_uuid,), fetch_one=True)
                    logger.warning(f"Stream {stream_id_str} failed to become active in StreamManager for WS. DB state: {db_state_after_start}")
                    await websocket.send_json({"status": "error", "message": "Stream failed to initialize. Check server logs."}); await websocket.close(1011); return
            except HTTPException as e_start:
                await websocket.send_json({"status": "error", "message": f"Failed to start stream: {e_start.detail}"}); await websocket.close(1011); return

        if not await stream_manager.connect_client_to_stream(stream_id_str, websocket):
            await websocket.send_json({"status": "error", "message": "Failed to connect to active stream process."}); await websocket.close(1011); return

        # UPDATED: Include location data in connection response
        await websocket.send_json({
            "status": "connected", 
            "message": f"Connected to stream: {s_name}",
            "stream_id": stream_id_str, 
            "owner": s_owner_username, 
            "workspace_id": str(s_workspace_id), 
            "your_role": requester_role_info.get("role"),
            # NEW: Location information
            "location_info": location_data
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        target_fps = config.get("websocket_client_fps", 15.0) # Match stream_one.py (15fps)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066 # (approx 15fps)

        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            async with stream_manager._lock: 
                stream_info = stream_manager.active_streams.get(stream_id_str, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        # Consider if frame_to_base64 should be in executor if it's slow
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(thread_pool, frame_to_base64, latest_frame_np)
            
            if not stream_ok:
                await websocket.send_json({"status":"info", "message":"Stream ended or became inactive."}); break
            
            if latest_frame_b64:
                await websocket.send_json({ # Match stream_one.py payload
                    "stream_id": stream_id_str, "frame": latest_frame_b64,
                    "timestamp": datetime.now(timezone.utc).timestamp()
                })
            await asyncio.sleep(target_frame_interval)
    except WebSocketDisconnect:
        logging.info(f"WS client disconnected from stream {stream_id_str or 'unknown'} (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"WS task for stream {stream_id_str or 'unknown'} cancelled (User: {user_id_for_log or 'unknown'}).")
    except ValueError as ve: 
        logging.warning(f"WS stream error (User: {user_id_for_log or 'unknown'}, Stream: {stream_id_str or 'unknown'}): Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Invalid stream ID format."}); await websocket.close(1008)
            except: pass # Ignore error on close if already closing
    except Exception as e:
        logging.error(f"WS stream error ({stream_id_str or 'unknown'}, User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}); await websocket.close(1011)
            except: pass
    finally:
        if ping_task and not ping_task.done(): ping_task.cancel()
        if stream_id_str: await stream_manager.disconnect_client(stream_id_str, websocket)
        try:
            # Ensure websocket is closed if not already
            if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
            # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
            # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
            # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        except RuntimeError:
            pass  # Ignore double-close errors

@router.websocket("/notify2")
async def websocket_notify(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None

    try:
        await websocket.accept()
        token = await session_manager.get_token_from_websocket(websocket)
        if not token: 
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token): # Payload match
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager.get_user_by_id(UUID(user_id_str)) # Fetch user for logging
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"

        logging.info(f"Notify WS: User {username_for_log} connected successfully")
            
        await websocket.send_json({ # Payload match
            "status": "connected", 
            "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get initial notifications
        initial_notifications = await stream_manager.get_notifications(user_id_str, workspace_id_filter=None, include_read=False, limit=20)
        
        # Format notifications
        formatted_notifications = []
        for notification in initial_notifications:
            formatted_notifications.append({
                "id": notification.get("notification_id"),
                "user_id": notification.get("user_id"),
                "workspace_id": notification.get("workspace_id"),
                "stream_id": notification.get("stream_id"),
                "camera_name": notification.get("camera_name"),
                "status": notification.get("status"),
                "message": notification.get("message"),
                "timestamp": notification.get("created_at").timestamp() if notification.get("created_at") else datetime.now(timezone.utc).timestamp(),
                "read": notification.get("is_read", False)
            })
        
        if formatted_notifications: # Payload match
            await websocket.send_json(formatted_notifications)  # Send as array directly
        
        # Subscribe to notifications
        subscription_success = await stream_manager.subscribe_to_notifications(user_id_str, websocket)
        if not subscription_success:
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            # Don't close here - let it continue
        
        ping_task = asyncio.create_task(send_ping(websocket))
        logging.info(f"Notify WS: Setup complete for {username_for_log}, entering message loop")

        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Timeout from config, matching stream_one.py key
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue 
                elif message.get("type") == "mark_read":
                    notif_ids_raw = message.get("notification_ids") # stream_one uses "notification_ids"
                    if isinstance(notif_ids_raw, list) and user_id_str:
                        updated_count = 0
                        valid_notif_ids_to_mark: List[UUID] = []
                        for nid_str_raw in notif_ids_raw:
                            try: valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                            except ValueError: logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
                        if valid_notif_ids_to_mark:
                            # Batched update would be more efficient for many IDs
                            for nid_uuid in valid_notif_ids_to_mark:
                                try:
                                    res = await db_manager.execute_query(
                                        "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                                        (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)), return_rowcount=True
                                    )
                                    if res and res > 0: updated_count +=1
                                except Exception as e_mark: logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        # Ack structure from stream_one.py
                        await websocket.send_json({"type": "ack_mark_read", "ids": notif_ids_raw, "updated_count": updated_count}) 

            except asyncio.TimeoutError: continue # No message from client, normal
            except WebSocketDisconnect: logging.info(f"Notify WS: Client {username_for_log} disconnected normally"); break # Client disconnected
            except asyncio.CancelledError: logging.info(f"Notify WS: Task cancelled for {username_for_log}"); raise # Propagate cancellation
            except Exception as e_recv:
                logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True); break
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
    except asyncio.CancelledError:
        logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}) # Payload match
            except: pass # Ignore error on close
    finally:
        # Cleanup
        logging.debug(f"Notify WS: Starting cleanup for {username_for_log or 'unknown'}")

        # Cancel ping task
        if ping_task and not ping_task.done(): 
            ping_task.cancel()
            try:
                await ping_task
            except asyncio.CancelledError:
                pass

        # Unsubscribe from notifications
        if user_id_str:
            try:
                await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
            except Exception as e_unsub:
                logging.error(f"Error unsubscribing {username_for_log}: {e_unsub}")

        # # Close websocket only if it's still connected
        # try:
        #     # Ensure websocket is closed if not already
        #     # if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        #     # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        #     # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        #     if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        # except Exception as e_close:
        #     logging.debug(f"Error closing websocket for {username_for_log}: {e_close}")
        
        await _safe_close_websocket(websocket, username_for_log)

        logging.debug(f"Notify WS: Connection cleanup for {username_for_log or 'unknown'}.")

@router.websocket("/notify1")
async def websocket_notify1(websocket: WebSocket):
    user_id_str: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None

    try:
        await websocket.accept()
        token = await session_manager.get_token_from_websocket(websocket)
        if not token: # Payload match stream_one.py
            await websocket.send_json({"status": "error", "message": "Authentication token required."}); await websocket.close(1008); return

        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token): # Payload match
            await websocket.send_json({"status": "error", "message": "Invalid or expired token."}); await websocket.close(1008); return
        
        user_id_str = token_data.user_id
        user_db_data = await user_manager.get_user_by_id(UUID(user_id_str)) # Fetch user for logging
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_str}"
            
        await websocket.send_json({ # Payload match
            "status": "connected", "message": "Connected to notification stream",
            "server_time": datetime.now(timezone.utc).timestamp()
        })
        
        # Get workspace_id from token if available, else None for all user's notifications.
        # stream_one.py's initial_notifications: workspace_id_filter=None, include_read=False, limit=20
        initial_notifications = await stream_manager.get_notifications(user_id_str, workspace_id_filter=None, include_read=False, limit=20)
        if initial_notifications: # Payload match
            await websocket.send_json({"type": "notifications_batch", "notifications": initial_notifications, "server_time": datetime.now(timezone.utc).timestamp()})
        
        if not await stream_manager.subscribe_to_notifications(user_id_str, websocket):
            logging.warning(f"Notify WS: Failed to subscribe {username_for_log} post-connection.")
            # Optionally close if subscription is critical, stream_one.py doesn't explicitly
        
        ping_task = asyncio.create_task(send_ping(websocket))

        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Timeout from config, matching stream_one.py key
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                if message.get("type") == "pong": 
                    logging.debug(f"Notification WS: Pong received from {username_for_log}")
                    continue 
                elif message.get("type") == "mark_read":
                    notif_ids_raw = message.get("notification_ids") # stream_one uses "notification_ids"
                    if isinstance(notif_ids_raw, list) and user_id_str:
                        updated_count = 0
                        valid_notif_ids_to_mark: List[UUID] = []
                        for nid_str_raw in notif_ids_raw:
                            try: valid_notif_ids_to_mark.append(UUID(str(nid_str_raw)))
                            except ValueError: logging.warning(f"Invalid notification ID format for mark_read from {username_for_log}: {nid_str_raw}")
                        
                        if valid_notif_ids_to_mark:
                            # Batched update would be more efficient for many IDs
                            for nid_uuid in valid_notif_ids_to_mark:
                                try:
                                    res = await db_manager.execute_query(
                                        "UPDATE notifications SET is_read = TRUE, updated_at = $1 WHERE notification_id = $2 AND user_id = $3 AND is_read = FALSE",
                                        (datetime.now(timezone.utc), nid_uuid, UUID(user_id_str)), return_rowcount=True
                                    )
                                    if res and res > 0: updated_count +=1
                                except Exception as e_mark: logger.error(f"Error marking notification {nid_uuid} as read for {user_id_str}: {e_mark}")
                        # Ack structure from stream_one.py
                        await websocket.send_json({"type": "ack_mark_read", "ids": notif_ids_raw}) # Send back original list of IDs processed

            except asyncio.TimeoutError: continue # No message from client, normal
            except WebSocketDisconnect: break # Client disconnected
            except asyncio.CancelledError: raise # Propagate cancellation
            except Exception as e_recv:
                logging.error(f"Notify WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True); break
    
    except WebSocketDisconnect:
        logging.info(f"Notify WS: Client {username_for_log or 'unknown'} disconnected.")
    except asyncio.CancelledError:
        logging.info(f"Notify WS task for {username_for_log or 'unknown'} cancelled.")
    except Exception as e_outer:
        logging.error(f"Notify WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.send_json({"status": "error", "message": "Internal server error."}) # Payload match
            except: pass # Ignore error on close
    finally:
        if user_id_str: await stream_manager.unsubscribe_from_notifications(user_id_str, websocket)
        if ping_task and not ping_task.done(): ping_task.cancel()
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        logging.debug(f"Notify WS: Connection cleanup for {username_for_log or 'unknown'}.")

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
        requester_role_info = await check_workspace_access(
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
        logging.error(f"Error updating stream thresholds: {e}", exc_info=True)
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
        requester_role_info = await check_workspace_access(
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
        logging.error(f"Error getting stream thresholds: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/workspaces/{workspace_id}/alert-streams")
async def get_workspace_alert_streams(
    workspace_id: str,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all streams with alerts enabled in a workspace."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        workspace_id_uuid = UUID(workspace_id)
        
        # Check workspace membership
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(requester_user_id_str), 
            workspace_id_uuid,
        )
        
        # Get streams with alerts enabled
        alert_streams = await db_manager.execute_query(
            """SELECT stream_id, name, location, area, building, zone,
                      count_threshold_greater, count_threshold_less, 
                      alert_enabled, status, is_streaming
               FROM video_stream 
               WHERE workspace_id = $1 AND alert_enabled = TRUE
               ORDER BY building, zone, area, location, name""",
            (workspace_id_uuid,), fetch_all=True
        )
        
        formatted_streams = [{
            "stream_id": str(s["stream_id"]),
            "name": s["name"],
            "location": s["location"],
            "area": s["area"],
            "building": s["building"],
            "zone": s["zone"],
            "status": s["status"],
            "is_streaming": s["is_streaming"],
            "thresholds": {
                "count_threshold_greater": s.get('count_threshold_greater'),
                "count_threshold_less": s.get('count_threshold_less'),
                "alert_enabled": s.get('alert_enabled', False)
            }
        } for s in alert_streams or []]
        
        return JSONResponse(content={
            "status": "success",
            "workspace_id": workspace_id,
            "alert_streams": formatted_streams,
            "total": len(formatted_streams)
        })
        
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workspace ID format")
    except Exception as e:
        logging.error(f"Error getting workspace alert streams: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

# === LOCATION-BASED STREAM MANAGEMENT ENDPOINTS ===

@router.post("/streams/start-by-location")
async def start_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Start all streams matching the specified location criteria."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        # Build query to find matching streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id, vs.location, vs.area, 
                   vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = FALSE
        """
        
        params = [UUID(requester_user_id_str)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.workspace_id, vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        streams_to_consider = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        streams_to_consider = streams_to_consider or []
        
        if not streams_to_consider:
            return JSONResponse(content={
                "status": "info",
                "message": "No inactive streams found matching the specified location criteria",
                "filters_applied": {
                    "location": location,
                    "area": area,
                    "building": building,
                    "zone": zone,
                    "floor_level": floor_level,
                    "workspace_id": workspace_id
                },
                "streams_processed": 0,
                "streams_started": 0,
                "details": []
            })
        
        # Get current active stream counts per owner per workspace
        workspace_ids = list(set(str(s['workspace_id']) for s in streams_to_consider))
        workspace_placeholders = ','.join([f'${i+1}' for i in range(len(workspace_ids))])
        
        owner_active_streams_query = f"""
            SELECT user_id, workspace_id, COUNT(*) as active_count 
            FROM video_stream
            WHERE workspace_id IN ({workspace_placeholders}) AND is_streaming = TRUE 
            GROUP BY user_id, workspace_id
        """
        active_counts_db = await db_manager.execute_query(
            owner_active_streams_query, 
            tuple(UUID(wid) for wid in workspace_ids), 
            fetch_all=True
        )
        active_counts_db = active_counts_db or []
        
        # Create lookup map: (user_id, workspace_id) -> active_count
        owner_active_streams_map = {
            (row['user_id'], row['workspace_id']): row['active_count'] 
            for row in active_counts_db
        }
        
        results = []
        streams_started_count = 0
        streams_processed_count = len(streams_to_consider)
        
        for stream_data in streams_to_consider:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            owner_id = stream_data['user_id']
            ws_id = stream_data['workspace_id']
            
            # Check owner eligibility
            if not stream_data['owner_is_active']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream owner is inactive"
                })
                continue
                
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream owner not subscribed (and not admin)"
                })
                continue
            
            # Check camera limits
            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get((owner_id, ws_id), 0)
            
            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": f"Owner camera limit ({owner_limit}) reached"
                })
                continue
            
            # Start the stream
            await db_manager.execute_query(
                "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                (datetime.now(timezone.utc), stream_id)
            )
            
            streams_started_count += 1
            owner_active_streams_map[(owner_id, ws_id)] = current_owner_active_count + 1
            
            results.append({
                "stream_id": str(stream_id),
                "name": stream_name,
                "workspace": stream_data['workspace_name'],
                "location_info": {
                    "location": stream_data['location'],
                    "area": stream_data['area'],
                    "building": stream_data['building'],
                    "zone": stream_data['zone'],
                    "floor_level": stream_data['floor_level']
                },
                "status": "initiated",
                "message": "Stream start initiated"
            })
        
        status = "success" if streams_started_count == streams_processed_count else "partial_success"
        if streams_started_count == 0:
            status = "info"
            
        return JSONResponse(content={
            "status": status,
            "message": f"Started {streams_started_count} of {streams_processed_count} streams matching location criteria",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams_processed": streams_processed_count,
            "streams_started": streams_started_count,
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error starting streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })

@router.post("/streams/stop-by-location")
async def stop_streams_by_location(
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    workspace_id: Optional[str] = Query(None, description="Specific workspace ID (optional)"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Stop all streams matching the specified location criteria."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        # Build query to find matching active streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.user_id, vs.workspace_id, vs.location, vs.area,
                   vs.building, vs.zone, vs.floor_level, w.name as workspace_name
            FROM video_stream vs
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE
        """
        
        params = [UUID(requester_user_id_str)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.workspace_id, vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        streams_to_stop = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        streams_to_stop = streams_to_stop or []
        
        if not streams_to_stop:
            return JSONResponse(content={
                "status": "info",
                "message": "No active streams found matching the specified location criteria",
                "filters_applied": {
                    "location": location,
                    "area": area,
                    "building": building,
                    "zone": zone,
                    "floor_level": floor_level,
                    "workspace_id": workspace_id
                },
                "streams_processed": 0,
                "streams_stopped": 0,
                "details": []
            })
        
        results = []
        streams_stopped_count = 0
        streams_processed_count = len(streams_to_stop)
        
        for stream_data in streams_to_stop:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            owner_id = stream_data['user_id']
            ws_id = stream_data['workspace_id']
            
            updated_rows = await db_manager.execute_query(
                "UPDATE video_stream SET is_streaming = FALSE, status = 'inactive', updated_at = $1 WHERE stream_id = $2 AND is_streaming = TRUE",
                (datetime.now(timezone.utc), stream_id), return_rowcount=True
            )
            
            if updated_rows and updated_rows > 0:
                streams_stopped_count += 1
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "stopped",
                    "message": "Stream stop request processed"
                })
                
                # Send notification
                await stream_manager.add_notification(
                    str(owner_id), str(ws_id), str(stream_id), stream_name, 
                    "inactive", f"Camera '{stream_name}' stopped by {requester_username} (location-based action)"
                )
            else:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    },
                    "status": "skipped",
                    "message": "Stream already stopped or issue occurred"
                })
        
        status = "success" if streams_stopped_count == streams_processed_count else "partial_success"
        if streams_stopped_count == 0:
            status = "info"
            
        return JSONResponse(content={
            "status": status,
            "message": f"Stopped {streams_stopped_count} of {streams_processed_count} streams matching location criteria",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams_processed": streams_processed_count,
            "streams_stopped": streams_stopped_count,
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error stopping streams by location: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })

# === WEBSOCKET ENDPOINTS FOR LOCATION-BASED STREAMING ===

@router.websocket("/stream/by-location")
async def websocket_location_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming multiple cameras by location criteria."""
    user_id_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    connected_streams: Dict[str, bool] = {}
    
    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        
        # Extract location filters from query parameters
        location = query_params.get("location")
        area = query_params.get("area")
        building = query_params.get("building")
        zone = query_params.get("zone")
        floor_level = query_params.get("floor_level")
        workspace_id = query_params.get("workspace_id")
        
        # Validate that at least one filter is provided
        if not any([location, area, building, zone, floor_level]):
            await websocket.send_json({
                "status": "error", 
                "message": "At least one location filter (location, area, building, zone, floor_level) is required"
            })
            await websocket.close(1008)
            return
        
        # Authenticate user
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required"})
            await websocket.close(1008)
            return
            
        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Find matching streams
        base_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.is_streaming, vs.status,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_for_log)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.building, vs.zone, vs.area, vs.location, vs.name"
        
        matching_streams = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        matching_streams = matching_streams or []
        
        if not matching_streams:
            await websocket.send_json({
                "status": "error",
                "message": "No streams found matching the specified location criteria"
            })
            await websocket.close(1008)
            return
        
        # Connect to each matching stream
        active_streams = []
        for stream_data in matching_streams:
            stream_id_str = str(stream_data['stream_id'])
            
            # Try to connect to active streams or start inactive ones
            if stream_data['is_streaming'] and stream_data['status'] == 'active':
                if await stream_manager.connect_client_to_stream(stream_id_str, websocket):
                    connected_streams[stream_id_str] = True
                    active_streams.append({
                        "stream_id": stream_id_str,
                        "name": stream_data['name'],
                        "status": "connected",
                        "location_info": {
                            "location": stream_data['location'],
                            "area": stream_data['area'],
                            "building": stream_data['building'],
                            "zone": stream_data['zone'],
                            "floor_level": stream_data['floor_level']
                        }
                    })
                else:
                    active_streams.append({
                        "stream_id": stream_id_str,
                        "name": stream_data['name'],
                        "status": "connection_failed",
                        "location_info": {
                            "location": stream_data['location'],
                            "area": stream_data['area'],
                            "building": stream_data['building'],
                            "zone": stream_data['zone'],
                            "floor_level": stream_data['floor_level']
                        }
                    })
            else:
                active_streams.append({
                    "stream_id": stream_id_str,
                    "name": stream_data['name'],
                    "status": "inactive",
                    "location_info": {
                        "location": stream_data['location'],
                        "area": stream_data['area'],
                        "building": stream_data['building'],
                        "zone": stream_data['zone'],
                        "floor_level": stream_data['floor_level']
                    }
                })
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": f"Connected to location-based stream",
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            },
            "streams": active_streams,
            "total_streams": len(matching_streams),
            "connected_streams": len([s for s in active_streams if s["status"] == "connected"])
        })
        
        if not connected_streams:
            await websocket.send_json({
                "status": "info",
                "message": "No active streams to display. All streams in the specified location are inactive."
            })
            await websocket.close(1000)
            return
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Stream frames from all connected streams
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066
        
        while websocket.client_state == WebSocketState.CONNECTED and connected_streams:
            frames_data = []
            streams_still_active = {}
            
            # Collect frames from all connected streams
            async with stream_manager._lock:
                for stream_id_str in list(connected_streams.keys()):
                    stream_info = stream_manager.active_streams.get(stream_id_str, {})
                    
                    if stream_info and stream_info.get('status') == 'active':
                        streams_still_active[stream_id_str] = True
                        latest_frame_np = stream_info.get('latest_frame')
                        
                        if latest_frame_np is not None:
                            try:
                                frame_b64 = await asyncio.get_event_loop().run_in_executor(
                                    thread_pool, frame_to_base64, latest_frame_np
                                )
                                
                                # Find stream details for this frame
                                stream_details = next(
                                    (s for s in matching_streams if str(s['stream_id']) == stream_id_str), 
                                    None
                                )
                                
                                frames_data.append({
                                    "stream_id": stream_id_str,
                                    "name": stream_details['name'] if stream_details else "Unknown",
                                    "frame": frame_b64,
                                    "location_info": {
                                        "location": stream_details['location'] if stream_details else None,
                                        "area": stream_details['area'] if stream_details else None,
                                        "building": stream_details['building'] if stream_details else None,
                                        "zone": stream_details['zone'] if stream_details else None,
                                        "floor_level": stream_details['floor_level'] if stream_details else None
                                    } if stream_details else {}
                                })
                            except Exception as e:
                                logging.error(f"Error processing frame for stream {stream_id_str}: {e}")
                    else:
                        # Stream is no longer active, remove from connected streams
                        await stream_manager.disconnect_client(stream_id_str, websocket)
            
            # Update connected streams to only include active ones
            connected_streams = streams_still_active
            
            if not connected_streams:
                await websocket.send_json({
                    "status": "info",
                    "message": "All streams have ended or become inactive"
                })
                break
            
            # Send frames if any are available
            if frames_data:
                await websocket.send_json({
                    "type": "multi_stream_frames",
                    "frames": frames_data,
                    "timestamp": datetime.now(timezone.utc).timestamp(),
                    "active_streams": len(connected_streams)
                })
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"Location-based WS client disconnected (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"Location-based WS task cancelled (User: {user_id_for_log or 'unknown'})")
    except ValueError as ve:
        logging.warning(f"Location-based WS error: Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid ID format"})
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"Location-based WS error (User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error"})
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        
        # Disconnect from all connected streams
        for stream_id_str in connected_streams:
            await stream_manager.disconnect_client(stream_id_str, websocket)
        
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()

@router.websocket("/stream/single-by-location")
async def websocket_single_location_stream(websocket: WebSocket):
    """WebSocket endpoint for streaming from the first available camera matching location criteria."""
    user_id_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    current_stream_id: Optional[str] = None
    
    try:
        await websocket.accept()
        query_params = dict(websocket.query_params)
        
        # Extract location filters
        location = query_params.get("location")
        area = query_params.get("area") 
        building = query_params.get("building")
        zone = query_params.get("zone")
        floor_level = query_params.get("floor_level")
        workspace_id = query_params.get("workspace_id")
        
        # Validate that at least one filter is provided
        if not any([location, area, building, zone, floor_level]):
            await websocket.send_json({
                "status": "error",
                "message": "At least one location filter is required"
            })
            await websocket.close(1008)
            return
        
        # Authenticate user
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({"status": "error", "message": "Authentication token required"})
            await websocket.close(1008)
            return
            
        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({"status": "error", "message": "Invalid or expired token"})
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Find the first matching active stream
        base_query = """
            SELECT vs.stream_id, vs.name, vs.workspace_id, vs.is_streaming, vs.status,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 AND vs.is_streaming = TRUE AND vs.status = 'active'
        """
        
        params = [UUID(user_id_for_log)]
        param_count = 1
        
        # Add location filters
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        if location:
            param_count += 1
            base_query += f" AND vs.location = ${param_count}"
            params.append(location)
        if area:
            param_count += 1
            base_query += f" AND vs.area = ${param_count}"
            params.append(area)
        if building:
            param_count += 1
            base_query += f" AND vs.building = ${param_count}"
            params.append(building)
        if zone:
            param_count += 1
            base_query += f" AND vs.zone = ${param_count}"
            params.append(zone)
        if floor_level:
            param_count += 1
            base_query += f" AND vs.floor_level = ${param_count}"
            params.append(floor_level)
            
        base_query += " ORDER BY vs.building, vs.zone, vs.area, vs.location, vs.name LIMIT 1"
        
        matching_stream = await db_manager.execute_query(base_query, tuple(params), fetch_one=True)
        
        if not matching_stream:
            await websocket.send_json({
                "status": "error",
                "message": "No active streams found matching the specified location criteria"
            })
            await websocket.close(1008)
            return
        
        current_stream_id = str(matching_stream['stream_id'])
        
        # Check workspace membership
        requester_role_info = await check_workspace_access(
            db_manager,
            UUID(user_id_for_log), 
            matching_stream['workspace_id'],
        )
        
        # Connect to the stream
        if not await stream_manager.connect_client_to_stream(current_stream_id, websocket):
            await websocket.send_json({
                "status": "error",
                "message": "Failed to connect to the stream"
            })
            await websocket.close(1011)
            return
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": f"Connected to stream: {matching_stream['name']}",
            "stream_id": current_stream_id,
            "stream_name": matching_stream['name'],
            "owner": matching_stream['owner_username'],
            "workspace_id": str(matching_stream['workspace_id']),
            "location_info": {
                "location": matching_stream['location'],
                "area": matching_stream['area'],
                "building": matching_stream['building'],
                "zone": matching_stream['zone'],
                "floor_level": matching_stream['floor_level']
            },
            "filters_applied": {
                "location": location,
                "area": area,
                "building": building,
                "zone": zone,
                "floor_level": floor_level,
                "workspace_id": workspace_id
            }
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Stream frames
        target_fps = config.get("websocket_client_fps", 15.0)
        target_frame_interval = 1.0 / target_fps if target_fps > 0 else 0.066
        
        while websocket.client_state == WebSocketState.CONNECTED:
            latest_frame_b64 = None
            stream_ok = False
            
            async with stream_manager._lock:
                stream_info = stream_manager.active_streams.get(current_stream_id, {})
                if stream_info and stream_info.get('status') == 'active':
                    stream_ok = True
                    latest_frame_np = stream_info.get('latest_frame')
                    if latest_frame_np is not None:
                        latest_frame_b64 = await asyncio.get_event_loop().run_in_executor(
                            thread_pool, frame_to_base64, latest_frame_np
                        )
            
            if not stream_ok:
                await websocket.send_json({
                    "status": "info",
                    "message": "Stream ended or became inactive"
                })
                break
            
            if latest_frame_b64:
                await websocket.send_json({
                    "stream_id": current_stream_id,
                    "frame": latest_frame_b64,
                    "timestamp": datetime.now(timezone.utc).timestamp(),
                    "location_info": {
                        "location": matching_stream['location'],
                        "area": matching_stream['area'],
                        "building": matching_stream['building'],
                        "zone": matching_stream['zone'],
                        "floor_level": matching_stream['floor_level']
                    }
                })
            
            await asyncio.sleep(target_frame_interval)
            
    except WebSocketDisconnect:
        logging.info(f"Single location WS client disconnected (User: {user_id_for_log or 'unknown'})")
    except asyncio.CancelledError:
        logging.info(f"Single location WS task cancelled (User: {user_id_for_log or 'unknown'})")
    except ValueError as ve:
        logging.warning(f"Single location WS error: Invalid ID format - {ve}")
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Invalid ID format"})
                await websocket.close(1008)
            except:
                pass
    except Exception as e:
        logging.error(f"Single location WS error (User: {user_id_for_log or 'unknown'}): {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({"status": "error", "message": "Internal server error"})
                await websocket.close(1011)
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()
        
        if current_stream_id:
            await stream_manager.disconnect_client(current_stream_id, websocket)
        
        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()

# === ADDITIONAL UTILITY ENDPOINTS FOR LOCATION-BASED OPERATIONS ===

@router.get("/streams/locations/hierarchy")
async def get_location_hierarchy_for_streams(
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get the location hierarchy for streams with stream counts."""
    try:
        user_id_str = str(current_user_data["user_id"])
        
        base_query = """
            SELECT DISTINCT 
                vs.building, vs.floor_level, vs.zone, vs.area, vs.location,
                COUNT(*) as stream_count,
                COUNT(CASE WHEN vs.is_streaming = TRUE THEN 1 END) as active_streams,
                COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as healthy_streams
            FROM video_stream vs
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        
        base_query += """
            GROUP BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location
            ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location
        """
        
        hierarchy_data = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        hierarchy_data = hierarchy_data or []
        
        # Organize into hierarchy structure
        buildings = {}
        
        for row in hierarchy_data:
            building = row['building'] or 'Unknown Building'
            floor_level = row['floor_level'] or 'Unknown Floor'
            zone = row['zone'] or 'Unknown Zone'
            area = row['area'] or 'Unknown Area'
            location = row['location'] or 'Unknown Location'
            
            if building not in buildings:
                buildings[building] = {
                    'name': building,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'floors': {}
                }
            
            if floor_level not in buildings[building]['floors']:
                buildings[building]['floors'][floor_level] = {
                    'name': floor_level,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'zones': {}
                }
            
            if zone not in buildings[building]['floors'][floor_level]['zones']:
                buildings[building]['floors'][floor_level]['zones'][zone] = {
                    'name': zone,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'areas': {}
                }
            
            if area not in buildings[building]['floors'][floor_level]['zones'][zone]['areas']:
                buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area] = {
                    'name': area,
                    'total_streams': 0,
                    'active_streams': 0,
                    'healthy_streams': 0,
                    'locations': {}
                }
            
            # Add location data
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['locations'][location] = {
                'name': location,
                'total_streams': row['stream_count'],
                'active_streams': row['active_streams'],
                'healthy_streams': row['healthy_streams']
            }
            
            # Aggregate counts upward
            buildings[building]['total_streams'] += row['stream_count']
            buildings[building]['active_streams'] += row['active_streams']
            buildings[building]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['zones'][zone]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['zones'][zone]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['zones'][zone]['healthy_streams'] += row['healthy_streams']
            
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['total_streams'] += row['stream_count']
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['active_streams'] += row['active_streams']
            buildings[building]['floors'][floor_level]['zones'][zone]['areas'][area]['healthy_streams'] += row['healthy_streams']
        
        # Convert to list format
        hierarchy_list = []
        for building_name, building_data in buildings.items():
            building_item = {
                'type': 'building',
                'name': building_name,
                'total_streams': building_data['total_streams'],
                'active_streams': building_data['active_streams'],
                'healthy_streams': building_data['healthy_streams'],
                'children': []
            }
            
            for floor_name, floor_data in building_data['floors'].items():
                floor_item = {
                    'type': 'floor_level',
                    'name': floor_name,
                    'total_streams': floor_data['total_streams'],
                    'active_streams': floor_data['active_streams'],
                    'healthy_streams': floor_data['healthy_streams'],
                    'children': []
                }
                
                for zone_name, zone_data in floor_data['zones'].items():
                    zone_item = {
                        'type': 'zone',
                        'name': zone_name,
                        'total_streams': zone_data['total_streams'],
                        'active_streams': zone_data['active_streams'],
                        'healthy_streams': zone_data['healthy_streams'],
                        'children': []
                    }
                    
                    for area_name, area_data in zone_data['areas'].items():
                        area_item = {
                            'type': 'area',
                            'name': area_name,
                            'total_streams': area_data['total_streams'],
                            'active_streams': area_data['active_streams'],
                            'healthy_streams': area_data['healthy_streams'],
                            'children': []
                        }
                        
                        for location_name, location_data in area_data['locations'].items():
                            location_item = {
                                'type': 'location',
                                'name': location_name,
                                'total_streams': location_data['total_streams'],
                                'active_streams': location_data['active_streams'],
                                'healthy_streams': location_data['healthy_streams'],
                                'children': []
                            }
                            area_item['children'].append(location_item)
                        
                        zone_item['children'].append(area_item)
                    
                    floor_item['children'].append(zone_item)
                
                building_item['children'].append(floor_item)
            
            hierarchy_list.append(building_item)
        
        return JSONResponse(content={
            "status": "success",
            "hierarchy": hierarchy_list,
            "workspace_filter": workspace_id,
            "total_buildings": len(hierarchy_list),
            "summary": {
                "total_streams": sum(b['total_streams'] for b in hierarchy_list),
                "active_streams": sum(b['active_streams'] for b in hierarchy_list),
                "healthy_streams": sum(b['healthy_streams'] for b in hierarchy_list)
            }
        })
        
    except Exception as e:
        logging.error(f"Error getting location hierarchy: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": "Internal server error"
        })

@router.get("/streams/locations/search")
async def search_streams_by_location_filters(
    q: Optional[str] = Query(None, description="Search query for location names"),
    location_type: Optional[Union[str, List[str]]] = Query(None, description="Filter by location type(s): building, floor_level, zone, area, location"),
    locations: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific location(s)"),
    areas: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific area(s)"),
    buildings: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific building(s)"),
    floor_levels: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific floor level(s)"),
    zones: Optional[Union[str, List[str]]] = Query(None, description="Filter by specific zone(s)"),
    workspace_id: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search for streams using location-based filters and text search."""
    
    # Parse all the filter parameters
    location_type = parse_string_or_list(location_type)
    locations = parse_string_or_list(locations)
    areas = parse_string_or_list(areas)
    buildings = parse_string_or_list(buildings)
    floor_levels = parse_string_or_list(floor_levels)
    zones = parse_string_or_list(zones)
    
    user_id_str = str(current_user_data["user_id"])
    username = current_user_data.get("username", "unknown")
    
    try:
        # Complete query to match /source/user fields
        base_query = """
            SELECT vs.stream_id, vs.user_id, u.username as owner_username, vs.name, vs.path, 
                   vs.type, vs.status, vs.is_streaming, vs.created_at, vs.updated_at,
                   vs.location, vs.area, vs.building, vs.floor_level, vs.zone, vs.latitude, vs.longitude,
                   vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled,
                   w.name as workspace_name, vs.workspace_id
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            LEFT JOIN workspaces w ON vs.workspace_id = w.workspace_id
            JOIN workspace_members wm ON vs.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1
        """
        
        params = [UUID(user_id_str)]
        param_count = 1
        
        if workspace_id:
            param_count += 1
            base_query += f" AND vs.workspace_id = ${param_count}"
            params.append(UUID(workspace_id))
        
        # Add text search with special handling for location_type + q combination
        if q:
            # Check if q contains comma-separated values and we have a specific location_type
            q_values = parse_string_or_list(q)
            
            if location_type and len(location_type) == 1 and q_values and len(q_values) > 1:
                # Handle specific case: location_type=areas&q=Area 1,Area 2
                location_field_map = {
                    "area": "vs.area",
                    "areas": "vs.area", 
                    "building": "vs.building",
                    "buildings": "vs.building",
                    "floor_level": "vs.floor_level",
                    "floor_levels": "vs.floor_level",
                    "zone": "vs.zone",
                    "zones": "vs.zone",
                    "location": "vs.location",
                    "locations": "vs.location"
                }
                
                field_name = location_field_map.get(location_type[0])
                if field_name:
                    # Use exact match for specific values
                    param_count += 1
                    q_placeholders = ", ".join([f"${param_count + i}" for i in range(len(q_values))])
                    base_query += f" AND {field_name} IN ({q_placeholders})"
                    params.extend(q_values)
                    param_count += len(q_values) - 1
                else:
                    # Fallback to general text search
                    param_count += 1
                    search_condition = f"""
                        AND (
                            vs.name ILIKE ${param_count} OR 
                            vs.building ILIKE ${param_count} OR 
                            vs.floor_level ILIKE ${param_count} OR 
                            vs.zone ILIKE ${param_count} OR 
                            vs.area ILIKE ${param_count} OR 
                            vs.location ILIKE ${param_count}
                        )
                    """
                    base_query += search_condition
                    params.append(f"%{q}%")
            else:
                # Regular text search
                param_count += 1
                search_condition = f"""
                    AND (
                        vs.name ILIKE ${param_count} OR 
                        vs.building ILIKE ${param_count} OR 
                        vs.floor_level ILIKE ${param_count} OR 
                        vs.zone ILIKE ${param_count} OR 
                        vs.area ILIKE ${param_count} OR 
                        vs.location ILIKE ${param_count}
                    )
                """
                base_query += search_condition
                params.append(f"%{q}%")
        
        # Add location type filter (now supports multiple types)
        if location_type:
            type_conditions = []
            for loc_type in location_type:
                if loc_type == "building":
                    type_conditions.append("(vs.building IS NOT NULL AND vs.building != '')")
                elif loc_type == "floor_level":
                    type_conditions.append("(vs.floor_level IS NOT NULL AND vs.floor_level != '')")
                elif loc_type == "zone":
                    type_conditions.append("(vs.zone IS NOT NULL AND vs.zone != '')")
                elif loc_type == "area":
                    type_conditions.append("(vs.area IS NOT NULL AND vs.area != '')")
                elif loc_type == "location":
                    type_conditions.append("(vs.location IS NOT NULL AND vs.location != '')")
            
            if type_conditions:
                base_query += f" AND ({' OR '.join(type_conditions)})"
        
        # Add specific location filters
        if locations:
            param_count += 1
            location_placeholders = ", ".join([f"${param_count + i}" for i in range(len(locations))])
            base_query += f" AND vs.location IN ({location_placeholders})"
            params.extend(locations)
            param_count += len(locations) - 1
        
        if areas:
            param_count += 1
            area_placeholders = ", ".join([f"${param_count + i}" for i in range(len(areas))])
            base_query += f" AND vs.area IN ({area_placeholders})"
            params.extend(areas)
            param_count += len(areas) - 1
        
        if buildings:
            param_count += 1
            building_placeholders = ", ".join([f"${param_count + i}" for i in range(len(buildings))])
            base_query += f" AND vs.building IN ({building_placeholders})"
            params.extend(buildings)
            param_count += len(buildings) - 1
        
        if floor_levels:
            param_count += 1
            floor_placeholders = ", ".join([f"${param_count + i}" for i in range(len(floor_levels))])
            base_query += f" AND vs.floor_level IN ({floor_placeholders})"
            params.extend(floor_levels)
            param_count += len(floor_levels) - 1
        
        if zones:
            param_count += 1
            zone_placeholders = ", ".join([f"${param_count + i}" for i in range(len(zones))])
            base_query += f" AND vs.zone IN ({zone_placeholders})"
            params.extend(zones)
        
        base_query += " ORDER BY u.username, vs.created_at DESC"
        
        # Rest of the function remains the same...
        results = await db_manager.execute_query(base_query, tuple(params), fetch_all=True)
        results = results or []
        
        # Format results to match /source/user response structure exactly
        formatted_results = [
            {
                "id": str(r["stream_id"]),
                "user_id": str(r["user_id"]),
                "owner_username": r["owner_username"],
                "name": r["name"],
                "path": r["path"],
                "type": r["type"],
                "status": r["status"],
                "is_streaming": r["is_streaming"],
                "location": r["location"],
                "area": r["area"],
                "building": r["building"],
                "floor_level": r["floor_level"],
                "zone": r["zone"],
                "latitude": float(r["latitude"]) if r["latitude"] else None,
                "longitude": float(r["longitude"]) if r["longitude"] else None,
                "count_threshold_greater": r["count_threshold_greater"],
                "count_threshold_less": r["count_threshold_less"],
                "alert_enabled": r["alert_enabled"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                "workspace_id": str(r["workspace_id"]) if r["workspace_id"] else None,
                "workspace_name": r["workspace_name"],
                "static_base64": encoded_string
            } for r in results
        ]
        
        return formatted_results
        
    except ValueError as ve:
        logger.error(f"Invalid data for location search for user {username}: {ve}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid data: {ve}")
    except Exception as e:
        logger.error(f"Error searching streams by location for user {username}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while searching streams.")


@router.post("/start_streams_bulk")
async def start_streams_bulk_endpoint(
    stream_ids: List[str] = Query(..., description="List of stream IDs to start"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Start multiple streams from a list of stream IDs."""
    try:
        requester_user_id_str = str(current_user_data["user_id"])
        requester_username = current_user_data["username"]
        
        if not stream_ids:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "No stream IDs provided"}
            )
        
        if len(stream_ids) > 50:  # Reasonable limit to prevent abuse
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Maximum 50 streams can be started at once"}
            )
        
        # Validate stream ID formats
        valid_stream_uuids = []
        invalid_ids = []
        
        for stream_id_str in stream_ids:
            try:
                valid_stream_uuids.append(UUID(stream_id_str))
            except ValueError:
                invalid_ids.append(stream_id_str)
        
        if invalid_ids:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error", 
                    "message": f"Invalid stream ID format(s): {', '.join(invalid_ids)}"
                }
            )
        
        # Get stream details with workspace and owner info
        placeholders = ','.join([f'${i+1}' for i in range(len(valid_stream_uuids))])
        
        streams_query = f"""
            SELECT vs.stream_id, vs.name, vs.user_id as owner_id, vs.workspace_id, vs.is_streaming,
                   vs.status, vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   u.username as owner_username, u.is_active as owner_is_active,
                   u.is_subscribed as owner_is_subscribed, u.count_of_camera as owner_camera_limit,
                   u.role as owner_system_role, w.name as workspace_name
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            JOIN workspaces w ON vs.workspace_id = w.workspace_id
            WHERE vs.stream_id IN ({placeholders})
        """
        
        streams_data = await db_manager.execute_query(
            streams_query, tuple(valid_stream_uuids), fetch_all=True
        )
        streams_data = streams_data or []
        
        # Check for missing streams
        found_stream_ids = {str(s['stream_id']) for s in streams_data}
        missing_stream_ids = [sid for sid in stream_ids if sid not in found_stream_ids]
        
        results = []
        streams_started_count = 0
        streams_processed_count = len(streams_data)
        
        # Add missing streams to results
        for missing_id in missing_stream_ids:
            results.append({
                "stream_id": missing_id,
                "name": "Unknown",
                "workspace": "Unknown",
                "status": "not_found",
                "message": "Stream not found"
            })
        
        if not streams_data:
            return JSONResponse(content={
                "status": "error" if missing_stream_ids else "info",
                "message": "No valid streams found to process",
                "streams_processed": 0,
                "streams_started": 0,
                "details": results
            })
        
        # Check workspace access for all streams
        workspace_access_checks = {}
        for stream_data in streams_data:
            workspace_id = stream_data['workspace_id']
            if workspace_id not in workspace_access_checks:
                try:
                    requester_role_info = await check_workspace_access(
                        db_manager,
                        UUID(requester_user_id_str), 
                        workspace_id,
                    )
                    workspace_access_checks[workspace_id] = True
                except HTTPException:
                    workspace_access_checks[workspace_id] = False
        
        # Get current active stream counts per owner per workspace
        workspace_ids = list(set(s['workspace_id'] for s in streams_data))
        if workspace_ids:
            ws_placeholders = ','.join([f'${i+1}' for i in range(len(workspace_ids))])
            owner_active_streams_query = f"""
                SELECT user_id, workspace_id, COUNT(*) as active_count 
                FROM video_stream
                WHERE workspace_id IN ({ws_placeholders}) AND is_streaming = TRUE 
                GROUP BY user_id, workspace_id
            """
            active_counts_db = await db_manager.execute_query(
                owner_active_streams_query, tuple(workspace_ids), fetch_all=True
            )
            active_counts_db = active_counts_db or []
            
            owner_active_streams_map = {
                (row['user_id'], row['workspace_id']): row['active_count'] 
                for row in active_counts_db
            }
        else:
            owner_active_streams_map = {}
        
        # Process each stream
        for stream_data in streams_data:
            stream_id = stream_data['stream_id']
            stream_name = stream_data['name']
            workspace_id = stream_data['workspace_id']
            owner_id = stream_data['owner_id']
            
            # Check workspace access
            if not workspace_access_checks.get(workspace_id, False):
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "access_denied",
                    "message": "No access to workspace or workspace not found"
                })
                continue
            
            # Check if already streaming
            if stream_data['is_streaming']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "already_streaming",
                    "message": "Stream is already active"
                })
                continue
            
            # Check owner eligibility
            if not stream_data['owner_is_active']:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": "Stream owner is inactive"
                })
                continue
            
            if not stream_data['owner_is_subscribed'] and stream_data['owner_system_role'] != 'admin':
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": "Stream owner not subscribed (and not admin)"
                })
                continue
            
            # Check camera limits
            owner_limit = stream_data['owner_camera_limit']
            current_owner_active_count = owner_active_streams_map.get((owner_id, workspace_id), 0)
            
            if stream_data['owner_system_role'] != 'admin' and current_owner_active_count >= owner_limit:
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "skipped",
                    "message": f"Owner camera limit ({owner_limit}) reached"
                })
                continue
            
            # Start the stream
            try:
                await db_manager.execute_query(
                    "UPDATE video_stream SET is_streaming = TRUE, status = 'processing', updated_at = $1 WHERE stream_id = $2",
                    (datetime.now(timezone.utc), stream_id)
                )
                
                streams_started_count += 1
                owner_active_streams_map[(owner_id, workspace_id)] = current_owner_active_count + 1
                
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "initiated",
                    "message": "Stream start initiated"
                })
                
                # Add success notification
                await stream_manager.add_notification(
                    str(owner_id), str(workspace_id), str(stream_id), stream_name,
                    "processing", f"Camera '{stream_name}' start initiated by {requester_username} (bulk action)"
                )
                
            except Exception as e:
                logging.error(f"Error starting stream {stream_id}: {e}", exc_info=True)
                results.append({
                    "stream_id": str(stream_id),
                    "name": stream_name,
                    "workspace": stream_data['workspace_name'],
                    "location_info": {
                        "location": stream_data.get('location'),
                        "area": stream_data.get('area'),
                        "building": stream_data.get('building'),
                        "zone": stream_data.get('zone'),
                        "floor_level": stream_data.get('floor_level')
                    },
                    "status": "error",
                    "message": f"Failed to start stream: {str(e)[:100]}"
                })
        
        # Determine overall status
        total_requested = len(stream_ids)
        total_found = len(streams_data)
        
        if streams_started_count == 0:
            status = "info" if total_found == 0 else "failed"
            message = "No streams were started"
        elif streams_started_count == total_requested:
            status = "success"
            message = f"All {streams_started_count} streams started successfully"
        else:
            status = "partial_success"
            message = f"Started {streams_started_count} of {total_requested} requested streams"
        
        return JSONResponse(content={
            "status": status,
            "message": message,
            "summary": {
                "total_requested": total_requested,
                "streams_found": total_found,
                "streams_started": streams_started_count,
                "missing_streams": len(missing_stream_ids),
                "requester": requester_username
            },
            "details": results
        })
        
    except Exception as e:
        logging.error(f"Error in bulk stream start: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Internal server error during bulk stream start",
                "details": []
            }
        )

@router.websocket("/start_streams_bulk")
async def websocket_start_streams_bulk(websocket: WebSocket):
    """WebSocket endpoint for bulk starting streams with real-time progress updates."""
    user_id_for_log: Optional[str] = None
    username_for_log: Optional[str] = None
    ping_task: Optional[asyncio.Task] = None
    
    try:
        await websocket.accept()
        
        # Authenticate user
        token = await session_manager.get_token_from_websocket(websocket)
        if not token:
            await websocket.send_json({
                "status": "error", 
                "message": "Authentication token required."
            })
            await websocket.close(1008)
            return
            
        token_data = await session_manager.verify_token(token, "access")
        if not token_data or await session_manager.is_token_blacklisted(token):
            await websocket.send_json({
                "status": "error", 
                "message": "Invalid or expired token."
            })
            await websocket.close(1008)
            return
            
        user_id_for_log = token_data.user_id
        
        # Get user data for logging
        user_db_data = await user_manager.get_user_by_id(UUID(user_id_for_log))
        username_for_log = user_db_data.get("username") if user_db_data else f"user_{user_id_for_log}"
        
        # Send connection confirmation
        await websocket.send_json({
            "status": "connected",
            "message": "Connected to bulk stream start service",
            "user": username_for_log,
            "timestamp": datetime.now(timezone.utc).timestamp()
        })
        
        ping_task = asyncio.create_task(send_ping(websocket))
        
        # Main message processing loop
        while websocket.client_state == WebSocketState.CONNECTED:
            try:
                # Wait for start request from client
                receive_timeout = float(config.get("websocket_receive_timeout", 45.0))
                message = await asyncio.wait_for(websocket.receive_json(), timeout=receive_timeout)
                
                # Handle different message types
                if message.get("type") == "pong":
                    logging.debug(f"Bulk start WS: Pong received from {username_for_log}")
                    continue
                
                elif message.get("type") == "start_streams":
                    stream_ids = message.get("stream_ids", [])
                    
                    if not stream_ids:
                        await websocket.send_json({
                            "type": "error",
                            "message": "No stream IDs provided",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                        continue
                    
                    if len(stream_ids) > 50:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Maximum 50 streams can be started at once",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                        continue
                    
                    # Process the bulk start request
                    await process_bulk_start_request(
                        websocket, stream_ids, user_id_for_log, username_for_log
                    )
                
                else:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Unknown message type: {message.get('type', 'none')}",
                        "timestamp": datetime.now(timezone.utc).timestamp()
                    })
                    
            except asyncio.TimeoutError:
                # No message from client, continue
                continue
            except WebSocketDisconnect:
                break
            except asyncio.CancelledError:
                raise
            except Exception as e_recv:
                logging.error(f"Bulk start WS: Error receiving from {username_for_log}: {e_recv}", exc_info=True)
                if websocket.client_state == WebSocketState.CONNECTED:
                    try:
                        await websocket.send_json({
                            "type": "error",
                            "message": "Error processing message",
                            "timestamp": datetime.now(timezone.utc).timestamp()
                        })
                    except:
                        pass
                break
                
    except WebSocketDisconnect:
        logging.info(f"Bulk start WS: Client {username_for_log or 'unknown'} disconnected")
    except asyncio.CancelledError:
        logging.info(f"Bulk start WS task for {username_for_log or 'unknown'} cancelled")
    except Exception as e_outer:
        logging.error(f"Bulk start WS: Outer error ({username_for_log or 'unknown'}): {e_outer}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try:
                await websocket.send_json({
                    "status": "error", 
                    "message": "Internal server error"
                })
            except:
                pass
    finally:
        if ping_task and not ping_task.done():
            ping_task.cancel()

        # Ensure websocket is closed if not already
        if websocket.client_state != WebSocketState.DISCONNECTED: await websocket.close()
        # if websocket.client_state not in (WebSocketState.DISCONNECTED, WebSocketState.CLOSING): await websocket.close()
        # if websocket.client_state != WebSocketState.CONNECTED: await websocket.close()
        # if websocket.client_state in (WebSocketState.CONNECTED, WebSocketState.CONNECTING): await websocket.close()
        logging.debug(f"Bulk start WS: Connection cleanup for {username_for_log or 'unknown'}")

@router.get("/debug/shared_streams")
async def debug_shared_streams():
    """Debug endpoint to check shared stream status"""
    if hasattr(stream_manager, 'video_file_manager'):
        stats = stream_manager.video_file_manager.get_all_stats()
        return {"shared_streams": stats}
    return {"error": "Video file manager not available"}

@router.post("/debug/force_restart_shared/{source_path}")
async def debug_force_restart_shared_stream(source_path: str):
    """Force restart a shared stream for debugging"""
    try:
        # URL decode the source path
        import urllib.parse
        decoded_source = urllib.parse.unquote(source_path)
        
        result = await stream_manager.force_restart_shared_stream(decoded_source)
        return {"success": result, "source": decoded_source}
    except Exception as e:
        return {"error": f"Failed to restart shared stream: {e}"}

@router.post("/debug/log_state")
async def debug_log_current_state():
    """Trigger logging of current stream manager state"""
    try:
        await stream_manager.log_stream_manager_state()
        return {"success": True, "message": "State logged to console"}
    except Exception as e:
        return {"error": f"Failed to log state: {e}"}

@router.get("/debug/validate_source/{source_path}")
async def debug_validate_source(source_path: str):
    """Validate a video source for debugging"""
    try:
        import urllib.parse
        decoded_source = urllib.parse.unquote(source_path)
        
        is_valid = await stream_manager._validate_stream_source(decoded_source)
        return {
            "source": decoded_source,
            "is_valid": is_valid,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": f"Failed to validate source: {e}"}


@router.get("/debug/stream/{stream_id}")
async def debug_stream_status(stream_id: str, response: Response):
    """Get detailed debug information for a specific stream"""
    try:
        # Validate stream_id format
        try:
            UUID(stream_id)
        except ValueError:
            response.status_code = 400
            return {"error": "Invalid stream_id format. Must be a valid UUID."}
        
        # Get the detailed status
        status = await stream_manager.get_detailed_stream_status(stream_id)
        
        # Additional validation that the response is JSON serializable
        import json
        try:
            json.dumps(status)
        except TypeError as json_error:
            logging.error(f"JSON serialization error in debug endpoint: {json_error}")
            return {
                "error": "Failed to serialize debug data",
                "stream_id": stream_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": str(json_error)
            }
        
        return status
        
    except Exception as e:
        logging.error(f"Error in debug_stream_status for stream {stream_id}: {e}", exc_info=True)
        response.status_code = 500
        return {
            "error": f"Failed to get stream status: {str(e)}",
            "stream_id": stream_id,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/debug/streams")
async def debug_all_streams():
    """Get debug information for all streams"""
    try:
        debug_info = await stream_manager.debug_all_streams()
        
        # Validate JSON serializability
        import json
        try:
            json.dumps(debug_info)
        except TypeError as json_error:
            logging.error(f"JSON serialization error in debug all streams: {json_error}")
            return {
                "error": "Failed to serialize all streams debug data",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "details": str(json_error)
            }
        
        return debug_info
        
    except Exception as e:
        logging.error(f"Error in debug_all_streams: {e}", exc_info=True)
        return {
            "error": f"Failed to get all streams debug info: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@router.get("/debug/video-sharing")
async def debug_video_sharing():
    """Get debug information about video sharing"""
    try:
        sharing_stats = await stream_manager.get_video_sharing_stats()
        
        # Add additional debug info
        debug_info = {
            **sharing_stats,
            "video_file_manager_exists": hasattr(stream_manager, 'video_file_manager'),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return debug_info
        
    except Exception as e:
        logging.error(f"Error in debug_video_sharing: {e}", exc_info=True)
        return {
            "error": f"Failed to get video sharing debug info: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    