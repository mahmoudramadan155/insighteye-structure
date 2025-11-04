# app/routes/elasticsearch_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, status, Response, status as http_status
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Union
from uuid import UUID
import logging
import io
import csv

from app.services.elasticsearch_service import elasticsearch_service
from app.services.user_service import user_manager
from app.services.session_service import session_manager
from app.schemas import (
    SearchQuery, LocationSearchQuery, DeleteDataRequest,
    CreateCollectionRequest, TimestampRangeResponse, CameraIdsResponse
)
from app.utils import parse_camera_ids, parse_string_or_list

logger = logging.getLogger(__name__)
router = APIRouter(tags=["elasticsearch_data"], prefix="/elasticsearch")

# ========== Search Endpoints ==========

@router.get("/workspace/search_results_v2")
async def workspace_search_results_v2(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: Optional[str] = Query(None),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data with optional filters"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Check search feature access
        can_search = user_db_info.get("is_search", False) or \
                    is_system_admin or \
                    (workspace_role in ["admin", "owner"])
        
        if not can_search:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Search feature unavailable for your account."
            )
        
        # Process per_page parameter
        processed_per_page = None
        if per_page and per_page.lower() not in ["none", "null", ""]:
            try:
                processed_per_page = int(per_page)
                if processed_per_page < 1 or processed_per_page > 100:
                    raise ValueError("Invalid range")
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="per_page must be between 1 and 100"
                )
        elif per_page is None:
            processed_per_page = 10
        
        # Build search query
        search_query = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute search
        results = await elasticsearch_service.search_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=processed_per_page,
            base64=base64
        )
        
        # Add search scope info
        results["search_scope"] = {
            "workspace_id": str(final_workspace_id),
            "index_queried": f"person_counts_ws_{str(final_workspace_id).replace('-', '_')}",
            "filters_applied": search_query.model_dump(exclude_none=True),
            "access_level": "system_admin" if is_system_admin else (workspace_role or "member"),
            "base64_frames_included": base64,
            "pagination_disabled": processed_per_page is None
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Workspace search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during workspace data search."
        )

@router.get("/workspace/search_results_v2_ordered")
async def workspace_search_results_v2_ordered(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data with timestamp ordering"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Build search query
        search_query = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Execute ordered search
        results = await elasticsearch_service.search_ordered_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=per_page
        )
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Ordered search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during ordered data search."
        )

@router.get("/workspace/location_search")
async def workspace_location_search(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    latitude: Optional[float] = Query(None),
    longitude: Optional[float] = Query(None),
    radius_km: Optional[float] = Query(None),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    base64: bool = Query(True),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search workspace data by location with optional geo-radius filtering"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Build location search query
        search_query = LocationSearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            location=location,
            area=area,
            building=building,
            floor_level=floor_level,
            zone=zone,
            latitude=latitude,
            longitude=longitude,
            radius_km=radius_km
        )
        
        # Execute location search
        results = await elasticsearch_service.search_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            page=page,
            per_page=per_page,
            base64=base64
        )

        # Add location filter info
        if "search_scope" not in results:
            results["search_scope"] = {}
        
        results["search_scope"]["location_filters"] = {
            "location": location,
            "area": area,
            "building": building,
            "floor_level": floor_level,
            "zone": zone,
            "latitude": latitude,
            "longitude": longitude,
            "radius_km": radius_km
        }
        
        return JSONResponse(content=results)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Location search error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error during location search."
        )

# ========== Statistics Endpoints ==========

@router.get("/workspace/timestamp_range")
async def get_timestamp_range(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get earliest and latest timestamps in workspace data"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get timestamp range
        result = await elasticsearch_service.get_timestamp_range(
            workspace_id=final_workspace_id,
            camera_ids=parse_camera_ids(camera_id_param) if camera_id_param else None,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result.model_dump())
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting timestamp range: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve timestamp range."
        )

@router.get("/workspace/camera_ids")
async def get_camera_ids(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique camera IDs in workspace"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get camera IDs
        cameras = await elasticsearch_service.get_camera_ids_for_workspace(final_workspace_id)
        
        return JSONResponse(content={
            "camera_ids": cameras,
            "count": len(cameras),
            "workspace_id": str(final_workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting camera IDs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve camera IDs."
        )

@router.get("/workspace/statistics")
async def get_workspace_statistics(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive statistics for workspace data"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Get statistics
        result = await elasticsearch_service.get_workspace_statistics(
            workspace_id=final_workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting workspace statistics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve workspace statistics."
        )

# ========== Data Management Endpoints ==========

@router.delete("/workspace/data")
async def delete_workspace_data(
    delete_request: DeleteDataRequest,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Delete data from workspace based on filters"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if delete_request.workspace_id:
            final_workspace_id = delete_request.workspace_id
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Only admins and workspace owners can delete data
        if not is_system_admin and workspace_role not in ["owner", "admin"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only workspace owners/admins can delete data."
            )
        
        # Execute deletion
        result = await elasticsearch_service.delete_workspace_data(
            workspace_id=final_workspace_id,
            delete_request=delete_request,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting workspace data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete workspace data."
        )

@router.get("/workspace/export/csv")
async def export_workspace_data_csv(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    camera_id_param: Optional[str] = Query(None, alias="camera_id"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    include_frame: bool = Query(False),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Export workspace data as CSV"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        # Build search query
        search_query = SearchQuery(
            camera_id=parse_camera_ids(camera_id_param) if camera_id_param else None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Get data for export
        csv_data, content_type, filename  = await elasticsearch_service.export_workspace_data(
            workspace_id=final_workspace_id,
            search_query=search_query,
            format='csv',
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            include_frame=include_frame
        )
        
        return Response(
            content=csv_data,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error exporting workspace data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to export workspace data."
        )

# ========== Camera Endpoints ==========

@router.get("/workspace/cameras")
async def get_workspace_cameras(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    floor_level: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    search_term: Optional[str] = Query(None),
    group_by: Optional[str] = Query(None, regex="^(location|area|building|floor_level|zone)$"),
    include_inactive: bool = Query(False),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get cameras in workspace with optional filtering and grouping"""
    try:
        requesting_user_id = current_user_data["user_id"]
        username = current_user_data["username"]
        
        user_db_info = await user_manager.get_user_by_id(requesting_user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        is_system_admin = (system_role == "admin")
        
        # Determine workspace
        if workspace_id_query:
            try:
                final_workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, active_ws_id = await elasticsearch_service.get_user_workspace_info(username)
            if not active_ws_id:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found.")
            final_workspace_id = active_ws_id
        
        # Check permissions
        workspace_role = None
        if not is_system_admin:
            try:
                member_info = await elasticsearch_service.check_workspace_membership(
                    requesting_user_id, final_workspace_id
                )
                workspace_role = member_info.get("role")
            except HTTPException as e:
                raise e
        
        location_filters = {
            'location': location,
            'area': area,
            'building': building,
            'floor_level': floor_level,
            'zone': zone
        }

        # Get cameras
        result = await elasticsearch_service.get_workspace_cameras(
            workspace_id=final_workspace_id,
            location_filters=location_filters,
            status=status,
            search_term=search_term,
            group_by=group_by,
            include_inactive=include_inactive,
            user_id=requesting_user_id,
            user_role=system_role
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting workspace cameras: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve workspace cameras."
        )

# ========== Location Hierarchy Endpoints ==========

@router.get("/elasticsearch/locations")
async def get_locations(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique locations in workspace"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"locations": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get locations
        location_data = await elasticsearch_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            location_field="location"
        )
        
        # Format response
        locations = [
            {
                "location": item["location"],
                "camera_count": item["camera_count"],
                "camera_ids": item.get("camera_ids", [])
            } for item in location_data if item.get("location")
        ]
        
        locations.sort(key=lambda x: x["location"])
        
        return JSONResponse(content={
            "locations": locations,
            "total_count": len(locations),
            "workspace_id": str(workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting locations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve locations."
        )

@router.get("/elasticsearch/areas")
async def get_areas(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique areas, optionally filtered by location"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"areas": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get areas
        filter_conditions = {}
        if location:
            filter_conditions["location"] = [location] if isinstance(location, str) else location
        
        area_data = await elasticsearch_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            location_field="area",
            filter_conditions=filter_conditions
        )
        
        areas = [
            {
                "area": item["area"],
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item.get("camera_ids", [])
            } for item in area_data if item.get("area")
        ]
        
        areas.sort(key=lambda x: (x["area"], x.get("location", "")))
        
        return JSONResponse(content={
            "areas": areas,
            "total_count": len(areas),
            "filtered_by_location": location,
            "workspace_id": str(workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting areas: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve areas."
        )

@router.get("/elasticsearch/buildings")
async def get_buildings(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique buildings, optionally filtered by location/area"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"buildings": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Build filters
        filter_conditions = {}
        if location:
            filter_conditions["location"] = [location]
        if area:
            filter_conditions["area"] = [area]
        
        building_data = await elasticsearch_service.get_location_data(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username,
            location_field="building",
            filter_conditions=filter_conditions
        )
        
        buildings = [
            {
                "building": item["building"],
                "area": item.get("area"),
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item.get("camera_ids", [])
            } for item in building_data if item.get("building")
        ]
        
        buildings.sort(key=lambda x: (x["building"], x.get("area", ""), x.get("location", "")))
        
        return JSONResponse(content={
            "buildings": buildings,
            "total_count": len(buildings),
            "filtered_by_area": area,
            "filtered_by_location": location,
            "workspace_id": str(workspace_id)
        })
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting buildings: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve buildings."
        )

@router.get("/elasticsearch/floor_levels")
async def get_floor_levels(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    location: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    building: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique floor levels, optionally filtered by location/area/building"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"floor_levels": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get floor levels
        result = await elasticsearch_service.get_unique_floor_levels(
            workspace_id=workspace_id,
            location=location,
            area=area,
            building=building,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting floor levels: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve floor levels."
        )

@router.get("/elasticsearch/zones")
async def get_zones(
    workspace_id_query: Optional[str] = Query(None, alias="workspaceId"),
    floor_level: Optional[str] = Query(None),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get all unique zones with their associated location information"""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        # Determine workspace
        if workspace_id_query:
            try:
                workspace_id = UUID(workspace_id_query)
            except ValueError:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid workspaceId format.")
        else:
            _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
            if not workspace_id:
                return {"zones": [], "total_count": 0}
        
        # Check permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Parse floor_level parameter
        parsed_floor_levels = None
        if floor_level:
            parsed_floor_levels = parse_string_or_list(floor_level)
        
        # Get zones
        result = await elasticsearch_service.get_unique_zones(
            workspace_id=workspace_id,
            floor_levels=parsed_floor_levels,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        # Format response to match qdrant router
        zone_data = result.get("zones", [])
        zones = [
            {
                "zone": item["zone"],
                "floor_level": item.get("floor_level"),
                "building": item.get("building"),
                "area": item.get("area"),
                "location": item.get("location"),
                "camera_count": item["camera_count"],
                "camera_ids": item["camera_ids"]
            } for item in zone_data if item.get("zone")
        ]
        
        zones.sort(key=lambda x: (
            x["zone"], 
            x.get("floor_level", ""), 
            x.get("building", ""), 
            x.get("area", ""), 
            x.get("location", "")
        ))
        
        return {
            "zones": zones,
            "total_count": len(zones),
            "filtered_by_floor_levels": parsed_floor_levels,
            "workspace_id": str(workspace_id)
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting zones: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve zones."
        )

@router.get("/elasticsearch/locations/analytics")
async def get_location_analytics(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    group_by: str = Query("location", regex="^(location|area|building|floor_level|zone)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get analytics data grouped by location hierarchy."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"analytics": [], "total_groups": 0, "group_by": group_by}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Build search query
        search_query = SearchQuery(
            camera_id=None,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # Get analytics
        result = await elasticsearch_service.get_location_analytics(
            workspace_id=workspace_id,
            search_query=search_query,
            group_by=group_by,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting location analytics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve location analytics."
        )

@router.get("/elasticsearch/locations/summary")
async def get_location_summary(
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Get comprehensive summary of all location data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
        if not workspace_id:
            return {
                "summary": {
                    "locations": 0, "areas": 0, "buildings": 0,
                    "floor_levels": 0, "zones": 0, "total_cameras": 0
                },
                "hierarchy": []
            }
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Get summary
        result = await elasticsearch_service.get_location_summary(
            workspace_id=workspace_id,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error getting location summary: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve location summary."
        )

@router.get("/elasticsearch/locations/search")
async def search_location_data(
    search_term: str = Query(..., min_length=1, description="Search term"),
    search_fields: List[str] = Query(
        default=["location", "area", "building", "floor_level", "zone"],
        description="Fields to search in"
    ),
    exact_match: bool = Query(False, description="Use exact match"),
    limit: int = Query(50, ge=1, le=200),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """Search location data."""
    try:
        username = current_user_data["username"]
        user_id = current_user_data["user_id"]
        
        user_db_info = await user_manager.get_user_by_id(user_id)
        if not user_db_info:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
        
        system_role = user_db_info.get("role")
        
        _, workspace_id = await elasticsearch_service.get_user_workspace_info(username)
        if not workspace_id:
            return {"results": [], "search_term": search_term}
        
        # Check workspace permissions
        workspace_role = None
        if system_role != "admin":
            member_info = await elasticsearch_service.check_workspace_membership(user_id, workspace_id)
            workspace_role = member_info.get("role")
        
        # Search
        result = await elasticsearch_service.search_location_data(
            workspace_id=workspace_id,
            search_term=search_term,
            search_fields=search_fields,
            exact_match=exact_match,
            limit=limit,
            user_system_role=system_role,
            user_workspace_role=workspace_role,
            requesting_username=username
        )
        
        return JSONResponse(content=result)
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error searching location data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to search location data."
        )