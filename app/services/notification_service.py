# app/services/notification_service.py
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID
from datetime import datetime, timedelta

from app.services.database import db_manager

logger = logging.getLogger(__name__)


class NotificationService:
    """
    Service layer for notification database operations.
    Encapsulates all notification logic and provides a clean API.
    """

    def __init__(self):
        self.db_manager = db_manager

    async def create_notification(
        self,
        workspace_id: UUID,
        user_id: UUID,
        status: str,
        message: str,
        stream_id: Optional[UUID] = None,
        camera_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new notification."""
        query = """
            INSERT INTO notifications 
            (workspace_id, user_id, stream_id, camera_name, status, message)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        """
        result = await self.db_manager.execute_query(
            query,
            (workspace_id, user_id, stream_id, camera_name, status, message),
            fetch_one=True,
        )
        
        if result:
            logger.debug(f"Notification created for user {user_id}: {message[:50]}...")
        
        return result

    async def get_notification_by_id(self, notification_id: UUID) -> Optional[Dict[str, Any]]:
        """Get a specific notification by ID."""
        query = "SELECT * FROM notifications WHERE notification_id = $1"
        return await self.db_manager.execute_query(query, (notification_id,), fetch_one=True)

    async def get_user_notifications(
        self, 
        user_id: UUID, 
        workspace_id: Optional[UUID] = None, 
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get notifications for a user with optional filters."""
        base_query = "SELECT * FROM notifications WHERE user_id = $1"
        params = [user_id]
        conditions = []
        
        if workspace_id:
            conditions.append(f"workspace_id = ${len(params) + 1}")
            params.append(workspace_id)
        
        if unread_only:
            conditions.append("is_read = FALSE")
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        base_query += " ORDER BY timestamp DESC"
        
        if limit:
            base_query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(base_query, tuple(params), fetch_all=True)

    async def get_workspace_notifications(
        self,
        workspace_id: UUID,
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all notifications for a workspace."""
        if unread_only:
            query = """
                SELECT * FROM notifications 
                WHERE workspace_id = $1 AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications 
                WHERE workspace_id = $1
                ORDER BY timestamp DESC
            """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

    async def get_stream_notifications(
        self,
        stream_id: UUID,
        unread_only: bool = False,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get all notifications for a specific stream."""
        if unread_only:
            query = """
                SELECT * FROM notifications 
                WHERE stream_id = $1 AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications 
                WHERE stream_id = $1
                ORDER BY timestamp DESC
            """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return await self.db_manager.execute_query(query, (stream_id,), fetch_all=True)

    async def mark_notification_as_read(self, notification_id: UUID) -> bool:
        """Mark a notification as read."""
        query = "UPDATE notifications SET is_read = TRUE WHERE notification_id = $1"
        rows = await self.db_manager.execute_query(query, (notification_id,), return_rowcount=True)
        
        if rows > 0:
            logger.debug(f"Marked notification {notification_id} as read")
        
        return rows > 0

    async def mark_all_notifications_as_read(
        self, 
        user_id: UUID, 
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Mark all notifications as read for a user."""
        if workspace_id:
            query = """
                UPDATE notifications SET is_read = TRUE 
                WHERE user_id = $1 AND workspace_id = $2 AND is_read = FALSE
            """
            params = (user_id, workspace_id)
        else:
            query = """
                UPDATE notifications SET is_read = TRUE 
                WHERE user_id = $1 AND is_read = FALSE
            """
            params = (user_id,)

        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Marked {rows} notifications as read for user {user_id}")
        
        return rows

    async def mark_stream_notifications_as_read(
        self,
        stream_id: UUID,
        user_id: Optional[UUID] = None
    ) -> int:
        """Mark all notifications for a stream as read."""
        if user_id:
            query = """
                UPDATE notifications SET is_read = TRUE
                WHERE stream_id = $1 AND user_id = $2 AND is_read = FALSE
            """
            params = (stream_id, user_id)
        else:
            query = """
                UPDATE notifications SET is_read = TRUE
                WHERE stream_id = $1 AND is_read = FALSE
            """
            params = (stream_id,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Marked {rows} notifications as read for stream {stream_id}")
        
        return rows

    async def delete_notification(self, notification_id: UUID) -> bool:
        """Delete a notification."""
        query = "DELETE FROM notifications WHERE notification_id = $1"
        rows = await self.db_manager.execute_query(query, (notification_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted notification {notification_id}")
        
        return rows > 0

    async def delete_user_notifications(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Delete all notifications for a user."""
        if workspace_id:
            query = "DELETE FROM notifications WHERE user_id = $1 AND workspace_id = $2"
            params = (user_id, workspace_id)
        else:
            query = "DELETE FROM notifications WHERE user_id = $1"
            params = (user_id,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications for user {user_id}")
        
        return rows

    async def delete_stream_notifications(self, stream_id: UUID) -> int:
        """Delete all notifications for a stream."""
        query = "DELETE FROM notifications WHERE stream_id = $1"
        rows = await self.db_manager.execute_query(query, (stream_id,), return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications for stream {stream_id}")
        
        return rows

    async def delete_old_notifications(
        self,
        days: int = 30,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Delete notifications older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        if workspace_id:
            query = """
                DELETE FROM notifications 
                WHERE workspace_id = $1 AND timestamp < $2
            """
            params = (workspace_id, cutoff_date)
        else:
            query = "DELETE FROM notifications WHERE timestamp < $1"
            params = (cutoff_date,)
        
        rows = await self.db_manager.execute_query(query, params, return_rowcount=True)
        
        if rows > 0:
            logger.info(f"Deleted {rows} notifications older than {days} days")
        
        return rows

    async def get_unread_count(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> int:
        """Get count of unread notifications for a user."""
        if workspace_id:
            query = """
                SELECT COUNT(*) as count FROM notifications 
                WHERE user_id = $1 AND workspace_id = $2 AND is_read = FALSE
            """
            params = (user_id, workspace_id)
        else:
            query = """
                SELECT COUNT(*) as count FROM notifications 
                WHERE user_id = $1 AND is_read = FALSE
            """
            params = (user_id,)
        
        result = await self.db_manager.execute_query(query, params, fetch_one=True)
        return result.get("count", 0) if result else 0

    async def get_notification_summary(
        self,
        user_id: UUID,
        workspace_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """Get summary of notifications for a user."""
        if workspace_id:
            query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_read = FALSE THEN 1 END) as unread,
                    COUNT(CASE WHEN status = 'urgent' THEN 1 END) as urgent,
                    MAX(timestamp) as latest_notification
                FROM notifications
                WHERE user_id = $1 AND workspace_id = $2
            """
            params = (user_id, workspace_id)
        else:
            query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_read = FALSE THEN 1 END) as unread,
                    COUNT(CASE WHEN status = 'urgent' THEN 1 END) as urgent,
                    MAX(timestamp) as latest_notification
                FROM notifications
                WHERE user_id = $1
            """
            params = (user_id,)
        
        result = await self.db_manager.execute_query(query, params, fetch_one=True)
        
        if not result:
            return {
                "total": 0,
                "unread": 0,
                "urgent": 0,
                "latest_notification": None
            }
        
        return {
            "total": result.get("total", 0),
            "unread": result.get("unread", 0),
            "urgent": result.get("urgent", 0),
            "latest_notification": result.get("latest_notification")
        }

    async def get_urgent_notifications(
        self,
        workspace_id: UUID,
        unread_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Get all urgent notifications for a workspace."""
        if unread_only:
            query = """
                SELECT * FROM notifications
                WHERE workspace_id = $1 
                AND status = 'urgent' 
                AND is_read = FALSE
                ORDER BY timestamp DESC
            """
        else:
            query = """
                SELECT * FROM notifications
                WHERE workspace_id = $1 
                AND status = 'urgent'
                ORDER BY timestamp DESC
            """
        
        return await self.db_manager.execute_query(query, (workspace_id,), fetch_all=True)

notification_service = NotificationService()
