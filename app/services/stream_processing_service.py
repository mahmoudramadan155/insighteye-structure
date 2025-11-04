# app/services/stream_processing_service.py
"""
Stream Processing Service - Handles video frame processing, object detection, and alerts.
"""
import asyncio
import logging
import time
import cv2
import numpy as np
from typing import Dict, Optional, Any, Tuple
from uuid import UUID
from datetime import datetime, timezone, timedelta
from ultralytics import YOLO
import concurrent.futures
import os

from app.config.settings import config
from app.utils import send_people_count_alert_email, send_fire_alert_email
from app.services.database import db_manager

logger = logging.getLogger(__name__)

# ThreadPoolExecutor for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 1) * 2 + 4)
)


class StreamProcessingService:
    """
    Service for processing video streams - detection, alerts, and frame processing.
    """

    def __init__(self):
        self.db_manager = db_manager
        self.people_model = None
        self.gender_model = None
        self.fire_model = None
        self.stream_manager = None
        self.video_file_manager = None
        self.qdrant_service = None
        self._cached_results = {}
        
        # Initialize models
        self._initialize_models()
        
        logger.info("StreamProcessingService initialized")

    def initialize(self, stream_manager, video_file_manager, qdrant_service):
        """Initialize service with dependencies."""
        self.stream_manager = stream_manager
        self.video_file_manager = video_file_manager
        self.qdrant_service = qdrant_service
        logger.info("StreamProcessingService dependencies initialized")

    def _initialize_models(self):
        """Initialize YOLO models."""
        people_model_path = config.get("people_model_path", "yolov8n.pt")
        gender_model_path = config.get("gender_model_path", "gender.pt")
        fire_model_path = config.get("fire_model_path", "fire.pt")
        
        try:
            self.people_model = YOLO(people_model_path)
            self.gender_model = YOLO(gender_model_path)
            self.fire_model = YOLO(fire_model_path)
            logger.info("YOLO models initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize YOLO models: {e}", exc_info=True)
            self.people_model = YOLO("yolov8n.pt")
            self.gender_model = None
            self.fire_model = None

    def detect_objects_with_threshold(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.4,
        threshold_settings: Dict[str, Any] = None,
        stream_id_str: str = None
    ) -> Tuple[np.ndarray, int, bool, int, int, str]:
        """
        Detect objects in frame with threshold checking.
        Returns: (annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status)
        """
        if frame is None or frame.size == 0:
            return np.zeros((100, 100, 3), dtype=np.uint8), 0, False, 0, 0, "no detection"

        # Stream-specific frame counting
        if stream_id_str:
            frame_count = self.stream_manager.fire_detection_frame_counts.get(stream_id_str, 0) + 1
            self.stream_manager.fire_detection_frame_counts[stream_id_str] = frame_count
        else:
            frame_count = getattr(self, '_frame_count', 0) + 1
            setattr(self, '_frame_count', frame_count)
        
        # Cache setup
        cache_key = f"cache_{stream_id_str}" if stream_id_str else "cache_global"
        if cache_key not in self._cached_results:
            self._cached_results[cache_key] = {
                'male_count': 0,
                'female_count': 0,
                'fire_status': 'no detection',
                'last_gender_frame': 0,
                'last_fire_frame': 0
            }
        cache = self._cached_results[cache_key]

        # Resize frame if needed
        max_dim = config.get("yolo_max_input_dim", 640)
        h, w = frame.shape[:2]
        scale = 1.0
        
        if h > max_dim or w > max_dim:
            scale = max_dim / max(h, w)
            new_w, new_h = int(w * scale), int(h * scale)
            new_w = max(2, new_w - (new_w % 2))
            new_h = max(2, new_h - (new_h % 2))
            input_frame = cv2.resize(frame, (new_w, new_h), interpolation=cv2.INTER_AREA)
        else:
            input_frame = frame

        try:
            # People detection
            people_results = self.people_model.predict(
                source=input_frame, conf=conf_threshold, classes=[0], verbose=False
            )

            person_count = 0
            if people_results and people_results[0].boxes is not None:
                person_count = len(people_results[0].boxes)

            # Gender detection (every 3rd frame when people detected)
            if person_count > 0 and frame_count % 3 == 0 and self.gender_model:
                try:
                    gender_results = self.gender_model(source=input_frame, conf=0.3, verbose=False)
                    if gender_results and gender_results[0].boxes is not None:
                        male_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 1)
                        female_count = sum(1 for box in gender_results[0].boxes if int(box.cls[0]) == 0)
                        cache['male_count'] = male_count
                        cache['female_count'] = female_count
                        cache['last_gender_frame'] = frame_count
                except Exception as e:
                    logger.error(f"Gender detection error for stream {stream_id_str}: {e}")

            # Fire detection (every 10th frame)
            if frame_count % 10 == 0 and self.fire_model:
                try:
                    fire_results = self.fire_model(source=input_frame, conf=0.3, verbose=False)
                    
                    current_fire_status = "no detection"
                    if fire_results and fire_results[0].boxes is not None:
                        classes = [int(box.cls) for box in fire_results[0].boxes]
                        if 0 in classes:
                            current_fire_status = "fire"
                        elif 1 in classes:
                            current_fire_status = "smoke"

                    previous_fire_status = cache['fire_status']
                    cache['fire_status'] = current_fire_status
                    cache['last_fire_frame'] = frame_count
                    
                    # Log significant changes
                    if current_fire_status != previous_fire_status:
                        if current_fire_status in ["fire", "smoke"]:
                            logger.warning(f"🔥 Fire detection change: {stream_id_str} changed to '{current_fire_status}'")
                        else:
                            logger.warning(f"🌊 Fire cleared: {stream_id_str} changed to 'no detection'")
                    
                except Exception as e:
                    logger.error(f"Fire detection error for stream {stream_id_str}: {e}")

            # Use cached results
            male_count = cache['male_count']
            female_count = cache['female_count']
            fire_status = cache['fire_status']

            # Threshold checking for people count
            alert_triggered = False
            if threshold_settings and threshold_settings.get("alert_enabled", False):
                greater_than = threshold_settings.get("greater_than")
                less_than = threshold_settings.get("less_than")
                
                if greater_than is not None and person_count > greater_than:
                    alert_triggered = True
                if less_than is not None and person_count < less_than:
                    alert_triggered = True

            # Annotate frame
            annotated_frame = (
                people_results[0].plot(img=input_frame.copy())
                if people_results and people_results[0].boxes is not None
                else input_frame.copy()
            )
            
            # Add text overlays
            count_color = (0, 0, 255) if alert_triggered else (255, 255, 255)
            count_text = f"People: {person_count} | M: {male_count} | F: {female_count}"
            cv2.putText(
                annotated_frame, count_text, (10, 20),
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, count_color, 2, cv2.LINE_AA
            )
            
            # Fire/smoke overlay
            if fire_status != "no detection":
                fire_color = (0, 0, 255)
                fire_text = f"ALERT: {fire_status.upper()}"
                cv2.putText(
                    annotated_frame, fire_text, (10, 50),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, fire_color, 2, cv2.LINE_AA
                )
                
                # Blinking effect
                if frame_count % 20 < 10:
                    red_overlay = annotated_frame.copy()
                    red_overlay[:] = (0, 0, 255)
                    annotated_frame = cv2.addWeighted(annotated_frame, 0.9, red_overlay, 0.1, 0)

            # Alert styling for people threshold
            if alert_triggered:
                red_overlay = annotated_frame.copy()
                red_overlay[:] = (0, 0, 255)
                annotated_frame = cv2.addWeighted(annotated_frame, 0.85, red_overlay, 0.15, 0)
                
            # Scale back if needed
            if scale != 1.0:
                annotated_frame = cv2.resize(annotated_frame, (w, h), interpolation=cv2.INTER_LINEAR)
                
            return annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status
            
        except Exception as e:
            logger.error(f"Object detection error for stream {stream_id_str}: {e}", exc_info=True)
            return frame.copy(), 0, False, 0, 0, "no detection"

    async def _get_camera_threshold_settings(self, stream_id: UUID) -> Dict[str, Any]:
        """Get threshold settings for a camera stream."""
        query = """
            SELECT alert_enabled, count_threshold_greater, count_threshold_less
            FROM video_stream
            WHERE stream_id = $1
        """
        result = await self.db_manager.execute_query(query, (stream_id,), fetch_one=True)
        
        if not result:
            return {
                "alert_enabled": False,
                "greater_than": None,
                "less_than": None
            }
        
        return {
            "alert_enabled": result.get("alert_enabled", False),
            "greater_than": result.get("count_threshold_greater"),
            "less_than": result.get("count_threshold_less")
        }

    async def _validate_stream_source(self, source: str) -> bool:
        """Validate that the stream source is accessible."""
        loop = asyncio.get_event_loop()
        
        def check_source():
            cap = cv2.VideoCapture(source)
            is_valid = cap.isOpened()
            cap.release()
            return is_valid
        
        try:
            return await loop.run_in_executor(thread_pool, check_source)
        except Exception as e:
            logger.error(f"Error validating source {source}: {e}")
            return False

    async def _handle_people_count_alert(
        self,
        stream_id: UUID,
        stream_id_str: str,
        person_count: int,
        threshold_settings: Dict[str, Any],
        camera_name: str,
        workspace_id: UUID,
        owner_id: UUID
    ):
        """Handle people count alert logic."""
        try:
            # Get alert state
            from app.services.people_count_service import people_count_service
            alert_state = await people_count_service.get_people_count_alert_state(stream_id)
            
            current_time = datetime.now(timezone.utc)
            cooldown_minutes = 5
            
            # Determine threshold type
            threshold_type = None
            greater_than = threshold_settings.get("greater_than")
            less_than = threshold_settings.get("less_than")
            
            if greater_than is not None and person_count > greater_than:
                threshold_type = "greater_than"
            elif less_than is not None and person_count < less_than:
                threshold_type = "less_than"
            
            if not threshold_type:
                return
            
            # Check cooldown
            should_notify = False
            if not alert_state:
                should_notify = True
            else:
                last_notification = alert_state.get("last_notification_time")
                if last_notification:
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    if time_since_last >= cooldown_minutes:
                        should_notify = True
                else:
                    should_notify = True
            
            if should_notify:
                # Send notification
                from app.services.notification_service import notification_service
                message = f"People count alert: {person_count} people detected (threshold: {threshold_type})"
                
                await notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=owner_id,
                    status="unread",
                    message=message,
                    stream_id=stream_id,
                    camera_name=camera_name
                )
                
                # Send email
                await send_people_count_alert_email(
                    camera_name=camera_name,
                    count=person_count,
                    threshold_type=threshold_type,
                    threshold_value=greater_than if threshold_type == "greater_than" else less_than
                )
                
                # Update alert state
                await people_count_service.create_or_update_people_count_alert_state(
                    stream_id=stream_id,
                    last_count=person_count,
                    last_threshold_type=threshold_type,
                    last_notification_time=current_time
                )
                
                logger.info(f"People count alert sent for stream {stream_id_str}")
                
        except Exception as e:
            logger.error(f"Error handling people count alert: {e}", exc_info=True)

    async def _handle_fire_detection_alert(
        self,
        stream_id: UUID,
        stream_id_str: str,
        fire_status: str,
        camera_name: str,
        workspace_id: UUID,
        owner_id: UUID
    ):
        """Handle fire detection alert logic."""
        try:
            if fire_status == "no detection":
                return
            
            from app.services.fire_detection_service import fire_detection_service
            fire_state = await fire_detection_service.get_fire_detection_state(stream_id)
            
            current_time = datetime.now(timezone.utc)
            cooldown_minutes = 5
            
            # Check if we should send notification
            should_notify = False
            if not fire_state:
                should_notify = True
            else:
                last_fire_status = fire_state.get("fire_status")
                last_notification = fire_state.get("last_notification_time")
                
                # New detection or status change
                if last_fire_status != fire_status:
                    should_notify = True
                elif last_notification:
                    time_since_last = (current_time - last_notification).total_seconds() / 60
                    if time_since_last >= cooldown_minutes:
                        should_notify = True
                else:
                    should_notify = True
            
            if should_notify:
                # Send notification
                from app.services.notification_service import notification_service
                message = f"Fire detection alert: {fire_status.upper()} detected!"
                
                await notification_service.create_notification(
                    workspace_id=workspace_id,
                    user_id=owner_id,
                    status="urgent",
                    message=message,
                    stream_id=stream_id,
                    camera_name=camera_name
                )
                
                # Send email
                await send_fire_alert_email(
                    camera_name=camera_name,
                    fire_type=fire_status
                )
                
                # Update fire detection state
                await fire_detection_service.create_or_update_fire_detection_state(
                    stream_id=stream_id,
                    fire_status=fire_status,
                    last_detection_time=current_time,
                    last_notification_time=current_time
                )
                
                logger.warning(f"🔥 Fire alert sent for stream {stream_id_str}: {fire_status}")
                
        except Exception as e:
            logger.error(f"Error handling fire detection alert: {e}", exc_info=True)

    async def process_stream_with_sharing(
        self,
        stream_id: UUID,
        camera_name: str,
        source: str,
        owner_username: str,
        owner_id: UUID,
        workspace_id: UUID,
        stop_event: asyncio.Event,
        location_info: Optional[Dict[str, Any]] = None
    ):
        """
        Main stream processing loop with shared video source support.
        """
        frame_count = 0
        last_db_update_activity = datetime.now(timezone.utc)
        stream_id_str = str(stream_id)
        loop = asyncio.get_event_loop()
        shared_stream = None
        
        # Initialize fire detection state
        self.stream_manager.fire_detection_states[stream_id_str] = "no detection"
        self.stream_manager.fire_detection_frame_counts[stream_id_str] = 0
        
        logger.info(f"Starting stream processing for {stream_id_str} ({camera_name})")
        
        try:
            # Get threshold settings
            threshold_settings = await self._get_camera_threshold_settings(stream_id)
            
            # Validate source
            if not await self._validate_stream_source(source):
                raise RuntimeError(f"Invalid or inaccessible video source: {source}")
            
            # Get stream parameters
            from app.services.parameter_service import parameter_service
            params = await parameter_service.get_workspace_params(workspace_id)
            frame_skip = params.get("frame_skip", 300)
            frame_delay_target = params.get("frame_delay", 0.0)
            conf_threshold = params.get("conf", 0.4)
            
            # Check for shared stream
            shared_stream = self.stream_manager.get_shared_stream(source)
            if not shared_stream:
                shared_stream = self.stream_manager.create_shared_stream(
                    source, owner_username, stream_id
                )
                logger.info(f"Created new shared stream for source: {source}")
            else:
                logger.info(f"Using existing shared stream for source: {source}")
            
            # Register consumer
            shared_stream.register_consumer(stream_id_str)
            
            # Main processing loop
            while not stop_event.is_set():
                try:
                    # Get frame from shared stream
                    frame_data = await shared_stream.get_frame(timeout=5.0)
                    
                    if frame_data is None:
                        logger.warning(f"No frame data for stream {stream_id_str}")
                        await asyncio.sleep(0.1)
                        continue
                    
                    frame = frame_data.get("frame")
                    if frame is None or frame.size == 0:
                        await asyncio.sleep(0.1)
                        continue
                    
                    frame_count += 1
                    
                    # Process frame with detection
                    annotated_frame, person_count, alert_triggered, male_count, female_count, fire_status = \
                        await loop.run_in_executor(
                            thread_pool,
                            self.detect_objects_with_threshold,
                            frame,
                            conf_threshold,
                            threshold_settings,
                            stream_id_str
                        )
                    
                    # Update stream manager with processed frame
                    self.stream_manager.update_stream_frame(
                        stream_id_str,
                        annotated_frame,
                        person_count,
                        male_count,
                        female_count,
                        fire_status
                    )
                    
                    # Handle alerts
                    if alert_triggered and threshold_settings.get("alert_enabled"):
                        await self._handle_people_count_alert(
                            stream_id, stream_id_str, person_count,
                            threshold_settings, camera_name, workspace_id, owner_id
                        )
                    
                    if fire_status != "no detection":
                        await self._handle_fire_detection_alert(
                            stream_id, stream_id_str, fire_status,
                            camera_name, workspace_id, owner_id
                        )
                    
                    # Periodic database updates
                    current_time = datetime.now(timezone.utc)
                    if (current_time - last_db_update_activity).total_seconds() >= 30:
                        from app.services.video_stream_service import video_stream_service
                        await video_stream_service.update_stream_status(
                            stream_id, "active", is_streaming=True
                        )
                        last_db_update_activity = current_time
                    
                    # Frame delay
                    if frame_delay_target > 0:
                        await asyncio.sleep(frame_delay_target)
                    else:
                        await asyncio.sleep(0.01)
                    
                except asyncio.CancelledError:
                    logger.info(f"Stream processing cancelled for {stream_id_str}")
                    break
                except Exception as e:
                    logger.error(f"Error in processing loop for {stream_id_str}: {e}", exc_info=True)
                    await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Fatal error in stream processing for {stream_id_str}: {e}", exc_info=True)
            raise
        
        finally:
            # Cleanup
            logger.info(f"Cleaning up stream {stream_id_str}")
            
            if shared_stream:
                shared_stream.unregister_consumer(stream_id_str)
                if not shared_stream.has_consumers():
                    self.stream_manager.remove_shared_stream(source)
                    logger.info(f"Removed shared stream for source: {source}")
            
            # Clear cache
            cache_key = f"cache_{stream_id_str}"
            if cache_key in self._cached_results:
                del self._cached_results[cache_key]
            
            # Update database
            try:
                from app.services.video_stream_service import video_stream_service
                await video_stream_service.update_stream_status(
                    stream_id, "inactive", is_streaming=False
                )
            except Exception as e:
                logger.error(f"Error updating final stream status: {e}")
            
            logger.info(f"Stream processing completed for {stream_id_str}")

    async def process_single_frame(
        self,
        frame: np.ndarray,
        conf_threshold: float = 0.4
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Process a single frame and return annotated frame with detection data."""
        loop = asyncio.get_event_loop()
        
        annotated_frame, person_count, _, male_count, female_count, fire_status = \
            await loop.run_in_executor(
                thread_pool,
                self.detect_objects_with_threshold,
                frame,
                conf_threshold,
                None,
                None
            )
        
        detection_data = {
            "person_count": person_count,
            "male_count": male_count,
            "female_count": female_count,
            "fire_status": fire_status
        }
        
        return annotated_frame, detection_data

    def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up StreamProcessingService")
        self._cached_results.clear()
        
        # Shutdown thread pool
        thread_pool.shutdown(wait=False)

stream_processing_service = StreamProcessingService()
