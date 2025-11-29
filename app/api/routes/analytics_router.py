# app/routes/analytics_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import Optional, Dict
from datetime import date
from uuid import UUID
import logging

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.database import db_manager
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/cameras/unique")
async def get_unique_cameras(
    request: Request,
    order_by: str = Query("camera_name", regex="^(camera_name|camera_id)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get list of unique cameras
    
    Chart: Unique Cameras List
    Returns: List of distinct cameras with their IDs and names
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        query = """
            SELECT DISTINCT
                stream_id,
                camera_name,
                location,
                area,
                building,
                zone
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
            ORDER BY camera_name ASC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj)
            
        cameras = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(cameras),
            "data": cameras
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching unique cameras: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/frame-counts")
async def get_frame_counts_per_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(5000, le=10000, description="Max datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get total frame counts per camera
    
    Chart: Total Frames per Camera (Bar Chart)
    Returns: Number of records/frames for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                COUNT(*) AS total_frames,
                MIN(timestamp) AS first_frame,
                MAX(timestamp) AS last_frame
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id
            ORDER BY total_frames DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [dict(row) for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "limit_per_camera": limit,
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame counts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/average-people")
async def get_average_people_per_camera(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people recorded by each camera
    
    Chart: Average People per Camera (Bar Chart)
    Returns: Average person count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location
            ORDER BY avg_person_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average people: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/average-gender")
async def get_average_gender_per_camera(
    request: Request,
    gender: str = Query("male", regex="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of males or females recorded by each camera
    
    Chart: Average Gender Count per Camera (Bar Chart)
    Returns: Average male/female count for each camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                AVG({count_column}) AS avg_count,
                MAX({count_column}) AS max_count,
                MIN({count_column}) AS min_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND camera_name IS NOT NULL
              AND {count_column} IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location
            ORDER BY avg_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': float(row['avg_count']) if row['avg_count'] else 0,
            'gender': gender
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching average gender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/zones/gender-by-weekday")
async def get_gender_by_zone_and_weekday(
    request: Request,
    gender: str = Query("male", regex="^(male|female)$"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of males/females in each zone by weekday
    
    Chart: Gender Count by Zone and Weekday (Heatmap/Line Chart)
    Returns: Average gender count grouped by zone and day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        count_column = "male_count" if gender == "male" else "female_count"
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(zone, 'Unknown') AS zone,
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                AVG({count_column}) AS avg_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND date IS NOT NULL
              AND {count_column} IS NOT NULL
              {date_filter}
            GROUP BY zone, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, zone
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_count': float(row['avg_count']) if row['avg_count'] else 0,
            'gender': gender,
            'weekday_name': row['weekday_name'].strip()
        } for row in results]
        
        return {
            "success": True,
            "gender": gender,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching gender by zone/weekday: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/busiest-hours/by-weekday")
async def get_busiest_hour_per_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get the busiest hour for each weekday
    
    Chart: Busiest Hour by Weekday (Bar Chart)
    Returns: Hour with highest average person count for each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH hourly_avg AS (
                SELECT
                    TO_CHAR(date, 'Day') AS weekday_name,
                    EXTRACT(DOW FROM date) AS weekday_num,
                    EXTRACT(HOUR FROM time) AS hour,
                    AVG(person_count) AS avg_person_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND date IS NOT NULL
                  AND time IS NOT NULL
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date), EXTRACT(HOUR FROM time)
            ),
            ranked_hours AS (
                SELECT
                    weekday_name,
                    weekday_num,
                    hour,
                    avg_person_count,
                    ROW_NUMBER() OVER (PARTITION BY weekday_num ORDER BY avg_person_count DESC) AS rn
                FROM hourly_avg
            )
            SELECT
                weekday_name,
                weekday_num,
                hour,
                avg_person_count
            FROM ranked_hours
            WHERE rn = 1
            ORDER BY weekday_num
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching busiest hours: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/average-people")
async def get_average_people_by_floor(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level
    
    Chart: Average People by Floor (Bar Chart)
    Returns: Average person count for each floor level
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY floor_level
            ORDER BY floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor averages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-weekday")
async def get_people_by_floor_and_weekday(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and weekday
    
    Chart: People by Floor and Weekday (Grouped Bar/Line Chart)
    Returns: Average person count for each floor on each day of week
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                AVG(person_count) AS avg_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              AND date IS NOT NULL
              {date_filter}
            GROUP BY floor_level, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY weekday_num, floor_level
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'weekday_name': row['weekday_name'].strip(),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/weekday data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/floors/people-by-hour")
async def get_people_by_floor_and_hour(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get average number of people by floor level and hour
    
    Chart: People by Floor and Hour (Line Chart/Heatmap)
    Returns: Average person count for each floor at each hour of day
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(floor_level, 'Unknown') AS floor_level,
                EXTRACT(HOUR FROM time) AS hour,
                AVG(person_count) AS avg_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              AND time IS NOT NULL
              {date_filter}
            GROUP BY floor_level, EXTRACT(HOUR FROM time)
            ORDER BY floor_level, hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching floor/hour data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cameras/frame-comparison")
async def get_camera_frame_comparison(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare camera frame counts to average
    
    Chart: Camera Frame Count Comparison (Bar Chart with threshold line)
    Returns: Each camera's frame count compared to average (Above/Below/Average)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH frame_counts AS (
                SELECT
                    camera_name,
                    camera_id,
                    COUNT(*) AS frame_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND camera_name IS NOT NULL
                  {date_filter}
                GROUP BY camera_name, camera_id
            ),
            avg_frame AS (
                SELECT AVG(frame_count) AS avg_frame_count
                FROM frame_counts
            )
            SELECT
                f.camera_name,
                f.camera_id,
                f.frame_count,
                a.avg_frame_count,
                CASE
                    WHEN f.frame_count > a.avg_frame_count THEN 'Above Average'
                    WHEN f.frame_count < a.avg_frame_count THEN 'Below Average'
                    ELSE 'Average'
                END AS comparison,
                ROUND(((f.frame_count - a.avg_frame_count) / a.avg_frame_count * 100)::numeric, 2) AS percent_difference
            FROM frame_counts f
            CROSS JOIN avg_frame a
            ORDER BY f.frame_count DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_frame_count': float(row['avg_frame_count']) if row['avg_frame_count'] else 0,
            'percent_difference': float(row['percent_difference']) if row['percent_difference'] else 0
        } for row in results]
        
        # Calculate summary statistics
        total_cameras = len(data)
        above_avg = len([d for d in data if d['comparison'] == 'Above Average'])
        below_avg = len([d for d in data if d['comparison'] == 'Below Average'])
        
        return {
            "success": True,
            "summary": {
                "total_cameras": total_cameras,
                "above_average": above_avg,
                "below_average": below_avg,
                "at_average": total_cameras - above_avg - below_avg
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching frame comparison: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/timeseries/camera-data")
async def get_camera_timeseries(
    request: Request,
    camera_id: Optional[UUID] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(5000, le=5000, description="Max 5000 datapoints per camera"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Get time series data for line graph (limited to 5000 datapoints per camera)
    
    Chart: Time Series Line Graph
    Returns: Person count over time for specified camera(s)
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace. Please set an active workspace.")

        await check_workspace_access(
            db_manager,
            user_id_obj,
            workspace_id_obj,
            required_role=None,
        )
        
        params = [workspace_id_obj]
        filters = ["workspace_id = $1"]
        
        if camera_id:
            params.append(camera_id)
            filters.append(f"stream_id = ${len(params)}")
            
        if start_date:
            params.append(start_date)
            filters.append(f"date >= ${len(params)}")
        if end_date:
            params.append(end_date)
            filters.append(f"date <= ${len(params)}")
            
        where_clause = " AND ".join(filters)
        
        # Use window function to limit results per camera
        query = f"""
            WITH ranked_data AS (
                SELECT
                    stream_id,
                    camera_name,
                    timestamp,
                    person_count,
                    male_count,
                    female_count,
                    location,
                    ROW_NUMBER() OVER (
                        PARTITION BY stream_id 
                        ORDER BY timestamp DESC
                    ) AS rn
                FROM stream_results
                WHERE {where_clause}
            )
            SELECT
                stream_id,
                camera_name,
                timestamp,
                person_count,
                male_count,
                female_count,
                location
            FROM ranked_data
            WHERE rn <= ${len(params) + 1}
            ORDER BY stream_id, timestamp ASC
        """
        
        params.append(limit)
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Group by camera
        cameras_data = {}
        for row in results:
            cam_id = str(row['stream_id'])
            if cam_id not in cameras_data:
                cameras_data[cam_id] = {
                    "camera_id": cam_id,
                    "camera_name": row['camera_name'],
                    "location": row['location'],
                    "datapoints": []
                }
            cameras_data[cam_id]["datapoints"].append({
                "timestamp": row['timestamp'].isoformat(),
                "person_count": row['person_count'],
                "male_count": row['male_count'],
                "female_count": row['female_count']
            })
        
        return {
            "success": True,
            "limit_per_camera": limit,
            "cameras_count": len(cameras_data),
            "data": list(cameras_data.values())
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching timeseries data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
