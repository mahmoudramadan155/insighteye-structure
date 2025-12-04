# app/services/postgres_service.py
from fastapi import HTTPException, status
from typing import Dict, Any, Optional, Union, List, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta, time as dt_time
from collections import defaultdict
import logging
import numpy as np
import base64
import cv2
import json
import csv
import io

from app.services.database import db_manager
from app.utils import (
    parse_camera_ids, parse_date_format, parse_time_string,
    frame_to_base64
)
from app.schemas import (
    SearchQuery, TimestampRangeResponse,
    LocationSearchQuery, DeleteDataRequest,
    StreamUpdate
)

logger = logging.getLogger(__name__)


class PostgresService:
    """Service layer for all PostgreSQL database operations"""
    
    def __init__(self):
        self.db_manager = db_manager

    # ========== Detection Data Operations ==========
    
    async def insert_detection_data(
        self,
        stream_id: Union[str, UUID],
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        camera_name: str,
        username: str,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        frame: Optional[np.ndarray] = None,
        location_info: Optional[Dict[str, Any]] = None,
        save_frame: bool = True,
        result_id: Optional[UUID] = None
    ) -> bool:
        """Insert detection data into PostgreSQL (stream_results and optionally stream_frames)."""
        try:
            stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
            workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
            user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
            
            now_utc = datetime.now(timezone.utc)
            if result_id is None:
                result_id = uuid4()

            # ===== DATA VALIDATION: Ensure gender counts don't exceed person count =====
            # Validate and correct gender counts
            if male_count < 0:
                logger.warning(f"Invalid male_count ({male_count}) - setting to 0")
                male_count = 0
            if female_count < 0:
                logger.warning(f"Invalid female_count ({female_count}) - setting to 0")
                female_count = 0
            if person_count < 0:
                logger.warning(f"Invalid person_count ({person_count}) - setting to 0")
                person_count = 0
                
            # Check if gender sum exceeds person count
            gender_sum = male_count + female_count
            if gender_sum > person_count:
                logger.warning(
                    f"Gender count mismatch: male_count({male_count}) + female_count({female_count}) = "
                    f"{gender_sum} > person_count({person_count}). Adjusting gender counts proportionally."
                )

                person_count = gender_sum  # Update person_count to match gender sum
                logger.info(f"Adjusted to: male_count={male_count}, female_count={female_count}, person_count={person_count}")
            # ===========================================================================
            
            # Prepare location data
            location = location_info.get('location') if location_info else None
            area = location_info.get('area') if location_info else None
            building = location_info.get('building') if location_info else None
            zone = location_info.get('zone') if location_info else None
            floor_level = location_info.get('floor_level') if location_info else None
            latitude = float(location_info['latitude']) if location_info and location_info.get('latitude') else None
            longitude = float(location_info['longitude']) if location_info and location_info.get('longitude') else None
            
            async with self.db_manager.transaction() as conn:
                # Insert into stream_results
                result_query = """
                    INSERT INTO stream_results (
                        result_id, stream_id, workspace_id, user_id,
                        camera_id, camera_name, timestamp, date, time,
                        person_count, male_count, female_count, fire_status,
                        username, location, area, building, zone, floor_level,
                        latitude, longitude, created_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                        $14, $15, $16, $17, $18, $19, $20, $21, $22
                    )
                """
                
                await self.db_manager.execute_query(
                    result_query,
                    (
                        result_id, stream_id_obj, workspace_id_obj, user_id_obj,
                        str(stream_id_obj), camera_name,
                        now_utc, now_utc.date(), now_utc.time(),
                        person_count, male_count, female_count, fire_status,
                        username, location, area, building, zone, floor_level,
                        latitude, longitude, now_utc
                    ),
                    connection=conn
                )
                
                # Insert frame data if provided
                if frame is not None and save_frame:
                    try:
                        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
                        frame_base64 = base64.b64encode(buffer).decode('utf-8')
                        frame_size = len(frame_base64)
                        
                        frame_query = """
                            INSERT INTO stream_frames (
                                frame_id, result_id, stream_id,
                                frame_base64, frame_size_bytes, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6)
                        """
                        
                        await self.db_manager.execute_query(
                            frame_query,
                            (uuid4(), result_id, stream_id_obj, frame_base64, frame_size, now_utc),
                            connection=conn
                        )
                    except Exception as frame_error:
                        logger.warning(f"Failed to save frame: {frame_error}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error inserting detection data: {e}", exc_info=True)
            return False

    async def batch_insert_detection_data(
        self,
        detection_batch: List[Dict[str, Any]],
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """Batch insert multiple detection data points."""
        try:
            inserted_count = 0
            
            async with self.db_manager.transaction() as conn:
                for detection in detection_batch:
                    result_id = uuid4()
                    
                    # Process frame if provided
                    frame_base64 = None
                    if 'frame' in detection and detection['frame'] is not None:
                        frame_base64 = frame_to_base64(detection['frame'])
                    
                    # Insert result
                    result_query = """
                        INSERT INTO stream_results (
                            result_id, stream_id, workspace_id, user_id,
                            camera_id, camera_name, timestamp, date, time,
                            person_count, male_count, female_count, fire_status,
                            username, location, area, building, zone, floor_level,
                            latitude, longitude, created_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                            $14, $15, $16, $17, $18, $19, $20, $21, $22
                        )
                    """
                    
                    location_info = detection.get('location_info', {})
                    
                    await self.db_manager.execute_query(
                        result_query,
                        (
                            result_id,
                            UUID(detection['camera_id']),
                            workspace_id,
                            UUID(detection['user_id']),
                            detection['camera_id'],
                            detection['camera_name'],
                            datetime.fromtimestamp(detection['timestamp'], tz=timezone.utc),
                            datetime.fromisoformat(detection['date']).date(),
                            datetime.fromisoformat(detection['time']).time(),
                            detection['person_count'],
                            detection.get('male_count', 0),
                            detection.get('female_count', 0),
                            detection.get('fire_status', 'no detection'),
                            detection['username'],
                            location_info.get('location'),
                            location_info.get('area'),
                            location_info.get('building'),
                            location_info.get('zone'),
                            location_info.get('floor_level'),
                            float(location_info['latitude']) if location_info.get('latitude') else None,
                            float(location_info['longitude']) if location_info.get('longitude') else None,
                            datetime.now(timezone.utc)
                        ),
                        connection=conn
                    )
                    
                    # Insert frame if available
                    if frame_base64 and detection.get('save_frame', True):
                        frame_query = """
                            INSERT INTO stream_frames (
                                frame_id, result_id, stream_id,
                                frame_base64, frame_size_bytes, created_at
                            ) VALUES ($1, $2, $3, $4, $5, $6)
                        """
                        await self.db_manager.execute_query(
                            frame_query,
                            (uuid4(), result_id, UUID(detection['camera_id']), 
                             frame_base64, len(frame_base64), datetime.now(timezone.utc)),
                            connection=conn
                        )
                    
                    inserted_count += 1
            
            return {
                "success": True,
                "inserted_count": inserted_count,
                "database": "postgresql"
            }
            
        except Exception as e:
            logger.error(f"Error batch inserting: {e}", exc_info=True)
            return {"success": False, "error": str(e), "inserted_count": 0}

    # ========== Search Operations ==========
    
    async def search_workspace_data(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        page: int = 1,
        per_page: Optional[int] = 10,
        include_frame: bool = True
    ) -> Dict[str, Any]:
        """Search workspace data in PostgreSQL."""
        try:
            conditions = ["sr.workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # Build filters
            if search_query.camera_id:
                param_count += 1
                camera_ids = [str(c) for c in search_query.camera_id]
                conditions.append(f"sr.camera_id = ANY(${param_count})")
                params.append(camera_ids)
            
            # Date/time filters
            if search_query.start_date:
                start_date = parse_date_format(search_query.start_date)
                start_time = parse_time_string(
                    getattr(search_query, 'start_time', None), dt_time.min
                )
                start_datetime = datetime.combine(start_date, start_time).replace(tzinfo=timezone.utc)
                param_count += 1
                conditions.append(f"sr.timestamp >= ${param_count}")
                params.append(start_datetime)
            
            if search_query.end_date:
                end_date = parse_date_format(search_query.end_date)
                end_time = parse_time_string(
                    getattr(search_query, 'end_time', None),
                    dt_time.max.replace(microsecond=0)
                )
                end_datetime = datetime.combine(end_date, end_time).replace(tzinfo=timezone.utc)
                param_count += 1
                conditions.append(f"sr.timestamp <= ${param_count}")
                params.append(end_datetime)
            
            # Location filters
            for field in ['location', 'area', 'building', 'floor_level', 'zone']:
                if hasattr(search_query, field):
                    value = getattr(search_query, field)
                    if value:
                        param_count += 1
                        conditions.append(f"sr.{field} = ${param_count}")
                        params.append(value)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"sr.username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM stream_results sr WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, tuple(params), fetch_one=True
            )
            total_count = count_result['count']
            
            # Get paginated data
            if per_page is None:
                limit_clause = ""
                offset_clause = ""
                num_pages = 1
            else:
                num_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 0
                offset = (page - 1) * per_page
                limit_clause = f"LIMIT {per_page}"
                offset_clause = f"OFFSET {offset}"
            
            # Build frame join if needed
            frame_join = ""
            frame_select = ""
            if include_frame:
                frame_join = "LEFT JOIN stream_frames sf ON sr.result_id = sf.result_id"
                frame_select = ", sf.frame_base64"
            
            data_query = f"""
                SELECT sr.*, u.username as owner_username {frame_select}
                FROM stream_results sr
                JOIN users u ON sr.user_id = u.user_id
                {frame_join}
                WHERE {where_clause}
                ORDER BY sr.timestamp DESC
                {limit_clause} {offset_clause}
            """
            
            results = await self.db_manager.execute_query(
                data_query, tuple(params), fetch_all=True
            )
            
            paginated_data = []
            if results:
                for row in results:
                    paginated_data.append(self._format_result_data(row, include_frame))
            
            return {
                "data": paginated_data,
                "current_page": page if per_page is not None else 1,
                "num_of_pages": num_pages,
                "total_count": total_count,
                "per_page": per_page
            }
            
        except Exception as e:
            logger.error(f"Error searching workspace data: {e}", exc_info=True)
            raise

    # ========== Statistics Operations ==========
    
    async def get_workspace_statistics(
        self,
        workspace_id: UUID,
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a workspace."""
        try:
            conditions = ["sr.workspace_id = $1"]
            params = [workspace_id]
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                if requesting_username:
                    conditions.append("sr.username = $2")
                    params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            # Get statistics
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_points,
                    COUNT(DISTINCT sr.camera_id) as unique_cameras,
                    MIN(sr.timestamp) as first_timestamp,
                    MAX(sr.timestamp) as last_timestamp,
                    MIN(sr.date) as first_date,
                    MAX(sr.date) as last_date,
                    SUM(sr.person_count) as total_person_count,
                    AVG(sr.person_count) as avg_person_count,
                    MAX(sr.person_count) as max_person_count,
                    COUNT(CASE WHEN sr.fire_status != 'no detection' THEN 1 END) as fire_detections
                FROM stream_results sr
                WHERE {where_clause}
            """
            
            stats = await self.db_manager.execute_query(
                stats_query, tuple(params), fetch_one=True
            )
            
            # Get camera list
            camera_query = f"""
                SELECT DISTINCT sr.camera_id, sr.camera_name
                FROM stream_results sr       
                WHERE {where_clause}
                ORDER BY sr.camera_name
            """
            cameras = await self.db_manager.execute_query(
                camera_query, tuple(params), fetch_all=True
            )
            
            return {
                "workspace_id": str(workspace_id),
                "database": "postgresql",
                "statistics": {
                    "total_points": stats['total_points'],
                    "unique_cameras": stats['unique_cameras'],
                    "camera_list": [
                        {"id": str(c['camera_id']), "name": c['camera_name']}
                        for c in cameras
                    ] if cameras else [],
                    "time_range": {
                        "first_date": stats['first_date'].isoformat() if stats['first_date'] else None,
                        "last_date": stats['last_date'].isoformat() if stats['last_date'] else None,
                        "first_datetime": stats['first_timestamp'].isoformat() if stats['first_timestamp'] else None,
                        "last_datetime": stats['last_timestamp'].isoformat() if stats['last_timestamp'] else None
                    },
                    "detection_stats": {
                        "total_person_count": stats['total_person_count'] or 0,
                        "avg_person_count": float(stats['avg_person_count']) if stats['avg_person_count'] else 0,
                        "max_person_count": stats['max_person_count'] or 0,
                        "fire_detections": stats['fire_detections'] or 0
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting workspace statistics: {e}", exc_info=True)
            return {"workspace_id": str(workspace_id), "error": str(e), "statistics": {}}

    # ========== Export Operations ==========
    
    async def export_workspace_data(
        self,
        workspace_id: UUID,
        search_query: Optional[SearchQuery],
        format: str,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        include_frame: bool
    ) -> Tuple[bytes, str, str]:
        """Export workspace data in various formats."""
        try:
            # Build query with filters
            conditions = ["sr.workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            if search_query:
                # Add filters similar to search_workspace_data
                if search_query.camera_id:
                    param_count += 1
                    conditions.append(f"sr.camera_id = ANY(${param_count})")
                    params.append([str(c) for c in search_query.camera_id])
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"sr.username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            # Fetch all data
            frame_join = ""
            frame_select = ""
            if include_frame:
                frame_join = "LEFT JOIN stream_frames sf ON sr.result_id = sf.result_id"
                frame_select = ", sf.frame_base64"
            
            query = f"""
                SELECT sr.* {frame_select}
                FROM stream_results sr
                {frame_join}
                WHERE {where_clause}
                ORDER BY sr.timestamp DESC
            """
            
            results = await self.db_manager.execute_query(
                query, tuple(params), fetch_all=True
            )
            
            if format.lower() == 'csv':
                return self._export_as_csv(results, workspace_id)
            elif format.lower() == 'json':
                return self._export_as_json(results, workspace_id, include_frame)
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Error exporting workspace data: {e}", exc_info=True)
            raise

    def _export_as_csv(self, results: List, workspace_id: UUID) -> Tuple[bytes, str, str]:
        """Export results as CSV."""
        output = io.StringIO()
        
        if not results:
            return b"No data available", "text/csv", f"workspace_{workspace_id}_empty.csv"
        
        fieldnames = [
            'timestamp', 'date', 'time', 'camera_id', 'camera_name',
            'person_count', 'male_count', 'female_count', 'fire_status',
            'location', 'area', 'building', 'floor_level', 'zone',
            'latitude', 'longitude', 'username'
        ]
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in results:
            writer.writerow({
                'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                'date': row['date'].isoformat() if row['date'] else None,
                'time': row['time'].isoformat() if row['time'] else None,
                'camera_id': row['camera_id'],
                'camera_name': row['camera_name'],
                'person_count': row['person_count'],
                'male_count': row['male_count'],
                'female_count': row['female_count'],
                'fire_status': row['fire_status'],
                'location': row['location'],
                'area': row['area'],
                'building': row['building'],
                'floor_level': row['floor_level'],
                'zone': row['zone'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'username': row['username']
            })
        
        csv_data = output.getvalue().encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return csv_data, "text/csv", filename

    def _export_as_json(
        self, results: List, workspace_id: UUID, include_frame: bool
    ) -> Tuple[bytes, str, str]:
        """Export results as JSON."""
        data = []
        for row in results:
            row_dict = {
                'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                'date': row['date'].isoformat() if row['date'] else None,
                'time': row['time'].isoformat() if row['time'] else None,
                'camera_id': row['camera_id'],
                'camera_name': row['camera_name'],
                'person_count': row['person_count'],
                'male_count': row['male_count'],
                'female_count': row['female_count'],
                'fire_status': row['fire_status'],
                'location': row['location'],
                'area': row['area'],
                'building': row['building'],
                'floor_level': row['floor_level'],
                'zone': row['zone'],
                'latitude': float(row['latitude']) if row['latitude'] else None,
                'longitude': float(row['longitude']) if row['longitude'] else None,
                'username': row['username']
            }
            
            if include_frame and 'frame_base64' in row:
                row_dict['frame_base64'] = row['frame_base64']
            
            data.append(row_dict)
        
        json_data = json.dumps(data, indent=2).encode('utf-8')
        filename = f"workspace_{workspace_id}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return json_data, "application/json", filename

    # ========== Timestamp Operations ==========
    
    async def get_timestamp_range(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: Optional[str],
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> TimestampRangeResponse:
        """Get timestamp range for data."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            if camera_ids:
                param_count += 1
                conditions.append(f"camera_id = ANY(${param_count})")
                params.append(camera_ids)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    MIN(timestamp) as first_timestamp,
                    MAX(timestamp) as last_timestamp,
                    MIN(date) as first_date,
                    MAX(date) as last_date
                FROM stream_results
                WHERE {where_clause}
            """
            
            result = await self.db_manager.execute_query(
                query, tuple(params), fetch_one=True
            )
            
            response = TimestampRangeResponse()
            
            if result and result['first_timestamp'] and result['last_timestamp']:
                first_dt = result['first_timestamp']
                last_dt = result['last_timestamp']
                
                response.first_timestamp = first_dt.timestamp()
                response.last_timestamp = last_dt.timestamp()
                response.first_datetime = first_dt.isoformat()
                response.last_datetime = last_dt.isoformat()
                response.first_date = result['first_date'].isoformat()
                response.last_date = result['last_date'].isoformat()
                response.first_time = first_dt.time().isoformat(timespec='seconds')
                response.last_time = last_dt.time().isoformat(timespec='seconds')
            
            if camera_ids:
                response.camera_id = ", ".join(camera_ids) if len(camera_ids) > 1 else camera_ids[0]
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting timestamp range: {e}", exc_info=True)
            return TimestampRangeResponse()

    # ========== Location Operations ==========
    
    async def get_unique_locations(
        self,
        workspace_id: UUID,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        user_system_role: str = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: str = None
    ) -> Dict[str, Any]:
        """Get all unique locations with filtering options."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # Location filters
            if area:
                param_count += 1
                conditions.append(f"area = ${param_count}")
                params.append(area)
            if building:
                param_count += 1
                conditions.append(f"building = ${param_count}")
                params.append(building)
            if floor_level:
                param_count += 1
                conditions.append(f"floor_level = ${param_count}")
                params.append(floor_level)
            if zone:
                param_count += 1
                conditions.append(f"zone = ${param_count}")
                params.append(zone)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                if requesting_username:
                    param_count += 1
                    conditions.append(f"username = ${param_count}")
                    params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    location, area, building, floor_level, zone,
                    COUNT(DISTINCT camera_id) as camera_count,
                    ARRAY_AGG(DISTINCT camera_id) as camera_ids
                FROM stream_results
                WHERE {where_clause} AND location IS NOT NULL
                GROUP BY location, area, building, floor_level, zone
                ORDER BY location, area, building, floor_level, zone
            """
            
            results = await self.db_manager.execute_query(
                query, tuple(params), fetch_all=True
            )
            
            locations = []
            if results:
                for row in results:
                    locations.append({
                        'location': row['location'],
                        'area': row['area'],
                        'building': row['building'],
                        'floor_level': row['floor_level'],
                        'zone': row['zone'],
                        'camera_count': row['camera_count'],
                        'camera_ids': row['camera_ids']
                    })
            
            return {
                "locations": locations,
                "total_count": len(locations),
                "filtered_by_area": area,
                "filtered_by_building": building,
                "filtered_by_floor_level": floor_level,
                "filtered_by_zone": zone,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error getting unique locations: {e}", exc_info=True)
            raise

    # ========== Delete Operations ==========
    
    async def delete_data(
        self,
        workspace_id: UUID,
        delete_request: DeleteDataRequest,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Delete data from workspace."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # Build filters
            if delete_request.camera_id:
                param_count += 1
                camera_ids = parse_camera_ids(delete_request.camera_id)
                conditions.append(f"camera_id = ANY(${param_count})")
                params.append([str(c) for c in camera_ids])
            
            if delete_request.start_date:
                start_date = parse_date_format(delete_request.start_date)
                start_time = parse_time_string(
                    getattr(delete_request, 'start_time', None), dt_time.min
                )
                start_datetime = datetime.combine(start_date, start_time).replace(tzinfo=timezone.utc)
                param_count += 1
                conditions.append(f"timestamp >= ${param_count}")
                params.append(start_datetime)
            
            if delete_request.end_date:
                end_date = parse_date_format(delete_request.end_date)
                end_time = parse_time_string(
                    getattr(delete_request, 'end_time', None),
                    dt_time.max.replace(microsecond=0)
                )
                end_datetime = datetime.combine(end_date, end_time).replace(tzinfo=timezone.utc)
                param_count += 1
                conditions.append(f"timestamp <= ${param_count}")
                params.append(end_datetime)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"username = ${param_count}")
                params.append(requesting_username)
            
            if not conditions:
                raise ValueError("No filter criteria provided for deletion")
            
            where_clause = " AND ".join(conditions)
            
            # Get count before deletion
            count_query = f"SELECT COUNT(*) FROM stream_results WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, tuple(params), fetch_one=True
            )
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No data found matching criteria",
                    "deleted_count": 0
                }
            
            # Delete data (frames will cascade)
            delete_query = f"DELETE FROM stream_results WHERE {where_clause}"
            await self.db_manager.execute_query(delete_query, tuple(params))
            
            return {
                "status": "success",
                "message": f"Successfully deleted {count_before} data points",
                "deleted_count": count_before
            }
            
        except Exception as e:
            logger.error(f"Error deleting data: {e}", exc_info=True)
            raise

    async def delete_camera_data(
        self,
        camera_ids: List[str],
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """Delete all data for specific cameras."""
        try:
            query = """
                DELETE FROM stream_results 
                WHERE workspace_id = $1 AND camera_id = ANY($2)
            """
            
            result = await self.db_manager.execute_query(
                query, (workspace_id, camera_ids)
            )
            
            return {
                "success": True,
                "deleted_cameras": camera_ids,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error deleting camera data: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "deleted_cameras": []
            }

    async def delete_all_workspace_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Delete ALL data from a workspace."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                conditions.append("username = $2")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            # Get count
            count_query = f"SELECT COUNT(*) FROM stream_results WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, tuple(params), fetch_one=True
            )
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No data found to delete",
                    "deleted_count": 0,
                    "workspace_id": str(workspace_id)
                }
            
            # Delete
            delete_query = f"DELETE FROM stream_results WHERE {where_clause}"
            await self.db_manager.execute_query(delete_query, tuple(params))
            
            return {
                "status": "success",
                "message": "Successfully deleted all workspace data",
                "deleted_count": count_before,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error deleting all workspace data: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "deleted_count": 0,
                "workspace_id": str(workspace_id)
            }

    # ========== Metadata Update Operations ==========
    
    async def bulk_update_metadata(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        metadata_updates: Dict[str, Any],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Bulk update metadata for camera data points."""
        try:
            # Build SET clause
            set_parts = []
            params = [workspace_id]
            param_count = 1
            
            for field, value in metadata_updates.items():
                if value is not None and field in [
                    'camera_name', 'location', 'area', 'building',
                    'floor_level', 'zone', 'latitude', 'longitude'
                ]:
                    param_count += 1
                    set_parts.append(f"{field} = ${param_count}")
                    params.append(value)
            
            if not set_parts:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided",
                    "updated_count": 0
                }
            
            # Build WHERE clause
            conditions = ["workspace_id = $1"]
            
            if camera_ids:
                param_count += 1
                conditions.append(f"camera_id = ANY(${param_count})")
                params.append(camera_ids)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            set_clause = ", ".join(set_parts)
            
            # Get count before update
            count_query = f"SELECT COUNT(*) FROM stream_results WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, (workspace_id,) + tuple(params[1:-(len(set_parts))]), 
                fetch_one=True
            )
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": "No data points found matching criteria",
                    "updated_count": 0
                }
            
            # Perform update
            update_query = f"""
                UPDATE stream_results 
                SET {set_clause}
                WHERE {where_clause}
            """
            
            await self.db_manager.execute_query(update_query, tuple(params))
            
            return {
                "status": "success",
                "message": f"Successfully updated {count_before} data points",
                "updated_count": count_before,
                "updated_fields": list(metadata_updates.keys()),
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error bulk updating metadata: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "updated_count": 0
            }

    async def update_camera_metadata_by_id(
        self,
        workspace_id: UUID,
        camera_id: str,
        stream_update: StreamUpdate,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Update metadata for a specific camera."""
        try:
            # Build SET clause from StreamUpdate
            set_parts = []
            params = [workspace_id, camera_id]
            param_count = 2
            
            # Map StreamUpdate fields to database columns
            field_mapping = {
                'name': 'camera_name',
                'location': 'location',
                'area': 'area',
                'building': 'building',
                'floor_level': 'floor_level',
                'zone': 'zone',
                'latitude': 'latitude',
                'longitude': 'longitude'
            }
            
            for update_field, db_field in field_mapping.items():
                if hasattr(stream_update, update_field):
                    value = getattr(stream_update, update_field)
                    if value is not None:
                        param_count += 1
                        set_parts.append(f"{db_field} = ${param_count}")
                        params.append(value)
            
            if not set_parts:
                return {
                    "status": "info",
                    "message": "No valid metadata fields provided",
                    "updated_count": 0
                }
            
            # Build WHERE clause
            conditions = ["workspace_id = $1", "camera_id = $2"]
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            set_clause = ", ".join(set_parts)
            
            # Get count
            count_query = f"SELECT COUNT(*) FROM stream_results WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, (workspace_id, camera_id), fetch_one=True
            )
            count_before = count_result['count']
            
            if count_before == 0:
                return {
                    "status": "info",
                    "message": f"No data points found for camera {camera_id}",
                    "updated_count": 0
                }
            
            # Update
            update_query = f"""
                UPDATE stream_results 
                SET {set_clause}
                WHERE {where_clause}
            """
            
            await self.db_manager.execute_query(update_query, tuple(params))
            
            return {
                "status": "success",
                "message": f"Successfully updated camera {camera_id}",
                "updated_count": count_before,
                "camera_id": camera_id,
                "workspace_id": str(workspace_id)
            }
            
        except Exception as e:
            logger.error(f"Error updating camera metadata: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "updated_count": 0
            }

    # ========== Helper Methods ==========
    
    def _format_result_data(self, row: Dict, include_frame: bool = True) -> Dict[str, Any]:
        """Format result data for response."""
        data = {
            "id": str(row['result_id']),
            "metadata": {
                "camera_id": row['camera_id'],
                "name": row['camera_name'],
                "timestamp": row['timestamp'].timestamp() if row['timestamp'] else None,
                "date": row['date'].isoformat() if row['date'] else None,
                "time": row['time'].isoformat() if row['time'] else None,
                "person_count": row['person_count'],
                "male_count": row['male_count'],
                "female_count": row['female_count'],
                "fire_status": row['fire_status'],
                "owner_username": row.get('owner_username', row['username']),
                "location": row['location'],
                "area": row['area'],
                "building": row['building'],
                "floor_level": row['floor_level'],
                "zone": row['zone'],
                "latitude": float(row['latitude']) if row['latitude'] else None,
                "longitude": float(row['longitude']) if row['longitude'] else None
            }
        }
        
        if include_frame and 'frame_base64' in row:
            data['frame'] = row['frame_base64']
        
        return data

    async def get_camera_ids_for_workspace(self, workspace_id: UUID) -> List[Dict[str, str]]:
        """Get all camera IDs for a workspace."""
        try:
            query = """
                SELECT DISTINCT camera_id, camera_name 
                FROM stream_results 
                WHERE workspace_id = $1 
                ORDER BY camera_name
            """
            results = await self.db_manager.execute_query(
                query, (workspace_id,), fetch_all=True
            )
            
            if results:
                return [
                    {"id": row["camera_id"], "name": row["camera_name"]} 
                    for row in results
                ]
            return []
            
        except Exception as e:
            logger.error(f"Error getting camera IDs: {e}", exc_info=True)
            return []

    async def preview_metadata_update(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Preview data points that would be affected by a metadata update."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            if camera_ids:
                param_count += 1
                conditions.append(f"camera_id = ANY(${param_count})")
                params.append(camera_ids)
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                param_count += 1
                conditions.append(f"username = ${param_count}")
                params.append(requesting_username)
            
            where_clause = " AND ".join(conditions)
            
            # Get total count
            count_query = f"SELECT COUNT(*) FROM stream_results WHERE {where_clause}"
            count_result = await self.db_manager.execute_query(
                count_query, tuple(params), fetch_one=True
            )
            total_count = count_result['count']
            
            # Get unique cameras
            camera_query = f"""
                SELECT DISTINCT camera_id 
                FROM stream_results 
                WHERE {where_clause}
            """
            camera_results = await self.db_manager.execute_query(
                camera_query, tuple(params), fetch_all=True
            )
            unique_cameras = [r['camera_id'] for r in camera_results] if camera_results else []
            
            # Get sample points
            sample_query = f"""
                SELECT 
                    result_id, camera_id, camera_name, timestamp, date, time,
                    username, location, area, building, floor_level, zone,
                    latitude, longitude
                FROM stream_results
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
            
            samples = await self.db_manager.execute_query(
                sample_query, tuple(params), fetch_all=True
            )
            
            preview_data = []
            if samples:
                for row in samples:
                    preview_data.append({
                        "point_id": str(row['result_id']),
                        "camera_id": row['camera_id'],
                        "camera_name": row['camera_name'],
                        "timestamp": row['timestamp'].timestamp() if row['timestamp'] else None,
                        "date": row['date'].isoformat() if row['date'] else None,
                        "time": row['time'].isoformat() if row['time'] else None,
                        "username": row['username'],
                        "current_metadata": {
                            "location": row['location'],
                            "area": row['area'],
                            "building": row['building'],
                            "floor_level": row['floor_level'],
                            "zone": row['zone'],
                            "latitude": float(row['latitude']) if row['latitude'] else None,
                            "longitude": float(row['longitude']) if row['longitude'] else None
                        }
                    })
            
            return {
                "total_points_affected": total_count,
                "unique_cameras_affected": len(unique_cameras),
                "camera_ids_in_preview": unique_cameras,
                "preview_limit": limit,
                "sample_points": preview_data,
                "workspace_id": str(workspace_id),
                "database": "postgresql"
            }
            
        except Exception as e:
            logger.error(f"Error previewing metadata update: {e}", exc_info=True)
            return {
                "error": str(e),
                "total_points_affected": 0,
                "unique_cameras_affected": 0,
                "sample_points": []
            }

    async def search_location_data(
        self,
        workspace_id: UUID,
        search_term: str,
        search_fields: List[str],
        exact_match: bool,
        limit: int,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Search location data in PostgreSQL."""
        try:
            conditions = ["workspace_id = $1"]
            params = [workspace_id]
            param_count = 1
            
            # User access control
            if user_system_role != 'admin' and user_workspace_role not in ['admin', 'owner']:
                if requesting_username:
                    param_count += 1
                    conditions.append(f"username = ${param_count}")
                    params.append(requesting_username)
            
            # Valid location fields
            valid_fields = ["location", "area", "building", "floor_level", "zone"]
            search_fields_filtered = [f for f in search_fields if f in valid_fields]
            
            if not search_fields_filtered:
                return {
                    "results": [],
                    "search_term": search_term,
                    "search_fields": search_fields,
                    "exact_match": exact_match,
                    "total_matches": 0,
                    "workspace_id": str(workspace_id),
                    "database": "postgresql"
                }
            
            # Build search conditions
            search_conditions = []
            if exact_match:
                # Exact match for each field
                for field in search_fields_filtered:
                    param_count += 1
                    search_conditions.append(f"{field} = ${param_count}")
                    params.append(search_term)
            else:
                # ILIKE match for each field
                for field in search_fields_filtered:
                    param_count += 1
                    search_conditions.append(f"{field} ILIKE ${param_count}")
                    params.append(f"%{search_term}%")
            
            # Combine conditions
            if search_conditions:
                conditions.append(f"({' OR '.join(search_conditions)})")
            
            where_clause = " AND ".join(conditions)
            
            # Query to get matching locations with aggregated data
            query = f"""
                SELECT 
                    location, area, building, floor_level, zone,
                    COUNT(DISTINCT camera_id) as camera_count,
                    ARRAY_AGG(DISTINCT camera_id) as camera_ids,
                    ARRAY_AGG(DISTINCT 
                        CASE 
                            {' '.join([f"WHEN {field} = ${params.index(search_term) + 1 if exact_match else params.index(f'%{search_term}%') + 1} THEN '{field}'" for field in search_fields_filtered])}
                        END
                    ) FILTER (WHERE 
                        {' OR '.join([f"{field} {'=' if exact_match else 'ILIKE'} ${params.index(search_term) + 1 if exact_match else params.index(f'%{search_term}%') + 1}" for field in search_fields_filtered])}
                    ) as matched_fields
                FROM stream_results
                WHERE {where_clause}
                GROUP BY location, area, building, floor_level, zone
                ORDER BY camera_count DESC
                LIMIT {limit}
            """
            
            results = await self.db_manager.execute_query(
                query, tuple(params), fetch_all=True
            )
            
            # Format results
            matching_locations = []
            if results:
                for row in results:
                    # Filter out None values from matched_fields
                    matched_fields = [f for f in row['matched_fields'] if f] if row['matched_fields'] else []
                    
                    matching_locations.append({
                        'location': row['location'],
                        'area': row['area'],
                        'building': row['building'],
                        'floor_level': row['floor_level'],
                        'zone': row['zone'],
                        'camera_count': row['camera_count'],
                        'camera_ids': row['camera_ids'] or [],
                        'matched_fields': list(set(matched_fields))  # Remove duplicates
                    })
            
            return {
                "results": matching_locations,
                "search_term": search_term,
                "search_fields": search_fields,
                "exact_match": exact_match,
                "total_matches": len(matching_locations),
                "workspace_id": str(workspace_id),
                "database": "postgresql"
            }
            
        except Exception as e:
            logger.error(f"Error searching location data: {e}", exc_info=True)
            raise

    async def get_user_workspace_info(self, username: str) -> Tuple[Optional[UUID], Optional[UUID]]:
        """Retrieve user ID and their active workspace."""
        try:
            if not username:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail="Username is required"
                )

            # Get user ID
            query_user = "SELECT user_id FROM users WHERE username = $1"
            user_result = await self.db_manager.execute_query(
                query_user, (username,), fetch_one=True
            )

            if not user_result or not user_result.get("user_id"):
                logger.warning(f"User ID lookup failed for username: {username}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail="User not found"
                )

            user_id = UUID(str(user_result["user_id"]))

            # Simplified workspace lookup with single query using COALESCE
            workspace_query = """
                WITH session_workspace AS (
                    SELECT ut.workspace_id, ut.updated_at as last_used
                    FROM user_tokens ut
                    WHERE ut.user_id = $1 AND ut.is_active = TRUE
                    ORDER BY ut.updated_at DESC
                    LIMIT 1
                ),
                member_workspaces AS (
                    SELECT wm.workspace_id, wm.created_at as joined_at
                    FROM workspace_members wm
                    JOIN workspaces w ON wm.workspace_id = w.workspace_id
                    WHERE wm.user_id = $1 AND w.is_active = TRUE
                    ORDER BY wm.created_at ASC
                    LIMIT 1
                )
                SELECT COALESCE(
                    (SELECT workspace_id FROM session_workspace),
                    (SELECT workspace_id FROM member_workspaces)
                ) as workspace_id
            """
            
            ws_result = await self.db_manager.execute_query(
                workspace_query, (user_id,), fetch_one=True
            )

            workspace_id: Optional[UUID] = None
            if ws_result and ws_result.get("workspace_id"):
                workspace_id = UUID(str(ws_result["workspace_id"]))
            else:
                logger.warning(f"User {username} (ID: {user_id}) has no active workspace available.")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No active or available workspace found for user."
                )
            
            return user_id, workspace_id

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving user_id and workspace for username '{username}': {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve user and workspace information."
            )


# Global service instance
postgres_service = PostgresService()