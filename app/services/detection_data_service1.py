# In a new file: app/services/detection_data_service.py

from typing import Dict, Any, Optional, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone
import logging
import numpy as np

from app.services.qdrant_service import qdrant_service
from app.services.postgres_service import postgres_service
from app.utils import get_workspace_qdrant_collection_name

logger = logging.getLogger(__name__)

class DetectionDataService:
    """Unified service for handling detection data across PostgreSQL and Qdrant"""
    
    def __init__(self):
        self.qdrant_service = qdrant_service
        self.postgres_service = postgres_service
    
    async def insert_detection_data_unified(
        self,
        stream_id: UUID,
        workspace_id: UUID,
        user_id: UUID,
        camera_name: str,
        username: str,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        frame: Optional[np.ndarray] = None,
        location_info: Optional[Dict[str, Any]] = None,
        save_frame_in_postgres: bool = False,  # Option to save frame in both
        save_metadata_in_qdrant: bool = False   # Option to save metadata in both
    ) -> bool: #Tuple[bool, Optional[str]]:
        """
        Insert detection data into both PostgreSQL and Qdrant with synchronized IDs.
        
        Strategy:
        - Generate single result_id
        - Save metadata in PostgreSQL (stream_results)
        - Save frame in Qdrant (with optional metadata)
        - Optionally save frame in PostgreSQL too (stream_frames)
        
        Returns:
            Tuple of (success: bool, result_id: Optional[UUID])
        """
        
        # Generate single ID for both systems
        result_id = uuid4()
        
        try:
            # 1. Insert metadata into PostgreSQL first (it's the source of truth)
            pg_success = await self.postgres_service.insert_detection_data(
                stream_id=stream_id,
                workspace_id=workspace_id,
                user_id=user_id,
                camera_name=camera_name,
                username=username,
                person_count=person_count,
                male_count=male_count,
                female_count=female_count,
                fire_status=fire_status,
                frame=frame if save_frame_in_postgres else None,
                location_info=location_info,
                save_frame=save_frame_in_postgres,
                result_id=result_id  # Pass the generated ID
            )
            
            if not pg_success:
                logger.error(f"Failed to insert detection data into PostgreSQL for result_id {result_id}")
                return False
            
            # 2. Insert frame (and optionally metadata) into Qdrant
            if frame is not None:
                qdrant_success = await self.qdrant_service.insert_detection_data(
                    username=username,
                    camera_id_str=str(stream_id),
                    camera_name=camera_name,
                    count=person_count if save_metadata_in_qdrant else 0,  # Can save count or not
                    male_count=male_count if save_metadata_in_qdrant else 0,
                    female_count=female_count if save_metadata_in_qdrant else 0,
                    fire_status=fire_status if save_metadata_in_qdrant else "no detection",
                    frame=frame,
                    workspace_id=workspace_id,
                    location_info=location_info if save_metadata_in_qdrant else None,
                    result_id=result_id  # Use same ID
                )
                
                if not qdrant_success:
                    logger.warning(f"Failed to insert frame into Qdrant for result_id {result_id}")
                    # Don't fail the entire operation if Qdrant fails
            
            logger.info(f"Successfully inserted detection data with unified ID: {result_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error in unified detection data insertion: {e}", exc_info=True)
            return False
    
    async def retrieve_detection_data_unified(
        self,
        result_id: UUID,
        include_frame: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve detection data from both systems using unified ID.
        
        Returns combined data with metadata from PostgreSQL and frame from Qdrant.
        """
        try:
            # Get metadata from PostgreSQL
            metadata_query = """
                SELECT sr.*, u.username as owner_username
                FROM stream_results sr
                JOIN users u ON sr.user_id = u.user_id
                WHERE sr.result_id = $1
            """
            
            metadata = await self.postgres_service.db_manager.execute_query(
                metadata_query, (result_id,), fetch_one=True
            )
            
            if not metadata:
                logger.warning(f"No metadata found for result_id {result_id}")
                return None
            
            # Format metadata
            result = {
                "result_id": str(metadata['result_id']),
                "stream_id": str(metadata['stream_id']),
                "workspace_id": str(metadata['workspace_id']),
                "user_id": str(metadata['user_id']),
                "camera_id": metadata['camera_id'],
                "camera_name": metadata['camera_name'],
                "timestamp": metadata['timestamp'].isoformat() if metadata['timestamp'] else None,
                "date": metadata['date'].isoformat() if metadata['date'] else None,
                "time": metadata['time'].isoformat() if metadata['time'] else None,
                "person_count": metadata['person_count'],
                "male_count": metadata['male_count'],
                "female_count": metadata['female_count'],
                "fire_status": metadata['fire_status'],
                "username": metadata['username'],
                "owner_username": metadata.get('owner_username'),
                "location": metadata['location'],
                "area": metadata['area'],
                "building": metadata['building'],
                "floor_level": metadata['floor_level'],
                "zone": metadata['zone'],
                "latitude": float(metadata['latitude']) if metadata['latitude'] else None,
                "longitude": float(metadata['longitude']) if metadata['longitude'] else None,
                "created_at": metadata['created_at'].isoformat() if metadata['created_at'] else None
            }
            
            # Get frame from Qdrant if requested
            if include_frame:
                try:
                    client = self.qdrant_service.get_client()
                    collection_name = get_workspace_qdrant_collection_name(metadata['workspace_id'])
                    
                    # Retrieve point by ID
                    points = client.retrieve(
                        collection_name=collection_name,
                        ids=[str(result_id)],
                        with_payload=["frame_base64"],
                        with_vectors=False
                    )
                    
                    if points and len(points) > 0 and points[0].payload:
                        result["frame_base64"] = points[0].payload.get("frame_base64")
                    else:
                        logger.warning(f"No frame found in Qdrant for result_id {result_id}")
                        result["frame_base64"] = None
                        
                except Exception as qdrant_error:
                    logger.error(f"Error retrieving frame from Qdrant: {qdrant_error}")
                    result["frame_base64"] = None
            
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving unified detection data: {e}", exc_info=True)
            return None

# Global instance
detection_data_service = DetectionDataService()

"""
# In your stream processing code:
from app.services.detection_data_service import detection_data_service

# Insert data with unified ID
success = await detection_data_service.insert_detection_data_unified(
    stream_id=stream_id,
    workspace_id=workspace_id,
    user_id=user_id,
    camera_name=camera_name,
    username=username,
    person_count=person_count,
    male_count=male_count,
    female_count=female_count,
    fire_status=fire_status,
    frame=frame,
    location_info=location_info,
    save_frame_in_postgres=False,  # Only Qdrant stores frame
    save_metadata_in_qdrant=False  # Only PostgreSQL stores metadata
)

if success:
    logger.info(f"Data saved with ID: {result_id}")
    
    # Later, retrieve combined data
    combined_data = await detection_data_service.retrieve_detection_data_unified(
        result_id=result_id,
        include_frame=True
    )
"""
