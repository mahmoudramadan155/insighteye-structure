# app/services/postgres_service.py
from typing import List, Dict, Any, Optional, Union
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
import logging
from app.services.database import db_manager
from app.schemas import StreamUpdate

logger = logging.getLogger(__name__)


class PostgresService:
    """Service layer for all PostgreSQL database operations"""
    
    def __init__(self):
        self.db_manager = db_manager
        
    # ========== User Operations ==========
    
    async def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Get user details by username"""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                   is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE username = $1
        """
        user = await self.db_manager.execute_query(query, (username,), fetch_one=True)
        return dict(user) if user else None
    
    async def get_user_by_id(self, user_id: Union[str, UUID]) -> Optional[Dict]:
        """Get user details by user_id"""
        query = """
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                   is_subscribed, subscription_date, count_of_camera, is_search, is_prediction 
            FROM users 
            WHERE user_id = $1
        """
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        user = await self.db_manager.execute_query(query, (user_id_obj,), fetch_one=True)
        return dict(user) if user else None
    
    async def get_user_by_email(self, email: str) -> Optional[Dict]:
        """Get user details by email"""
        query = "SELECT * FROM users WHERE email = $1"
        user = await self.db_manager.execute_query(query, (email,), fetch_one=True)
        return dict(user) if user else None
    
    async def get_all_users(
        self, 
        include_inactive: bool = True,
        role_filter: Optional[str] = None
    ) -> List[Dict]:
        """Get all users with optional filters"""
        conditions = []
        params = []
        
        if not include_inactive:
            conditions.append("is_active = TRUE")
        
        if role_filter:
            conditions.append(f"role = ${len(params) + 1}")
            params.append(role_filter)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT user_id, username, email, created_at, is_active, last_login, role, 
                   is_subscribed, subscription_date, count_of_camera, is_search, is_prediction
            FROM users
            {where_clause}
            ORDER BY created_at DESC
        """
        
        users = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
        return [dict(user) for user in users] if users else []
    
    async def update_user_status(self, user_id: Union[str, UUID], is_active: bool) -> bool:
        """Update user active status"""
        query = "UPDATE users SET is_active = $1, updated_at = $2 WHERE user_id = $3"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, 
            (is_active, datetime.now(timezone.utc), user_id_obj), 
            return_rowcount=True
        )
        return rows_affected > 0
    
    async def update_user_role(self, user_id: Union[str, UUID], role: str) -> bool:
        """Update user role"""
        valid_roles = ['user', 'admin']
        if role not in valid_roles:
            raise ValueError(f"Invalid role. Must be one of: {valid_roles}")
        
        query = "UPDATE users SET role = $1, updated_at = $2 WHERE user_id = $3"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, 
            (role, datetime.now(timezone.utc), user_id_obj), 
            return_rowcount=True
        )
        return rows_affected > 0
    
    async def update_last_login(self, user_id: Union[str, UUID]) -> bool:
        """Update user's last login timestamp"""
        query = "UPDATE users SET last_login = $1 WHERE user_id = $2"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, 
            (datetime.now(timezone.utc), user_id_obj), 
            return_rowcount=True
        )
        return rows_affected > 0
    
    # ========== Workspace Operations ==========
    
    async def get_workspace_by_id(self, workspace_id: Union[str, UUID]) -> Optional[Dict]:
        """Get workspace details"""
        query = """
            SELECT workspace_id, name, description, created_at, updated_at, is_active
            FROM workspaces
            WHERE workspace_id = $1
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        workspace = await self.db_manager.execute_query(
            query, (workspace_id_obj,), fetch_one=True
        )
        return dict(workspace) if workspace else None
    
    async def get_user_workspaces(
        self, 
        user_id: Union[str, UUID],
        include_inactive: bool = False
    ) -> List[Dict]:
        """Get all workspaces for a user"""
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        active_filter = "" if include_inactive else "AND w.is_active = TRUE"
        
        query = f"""
            SELECT w.workspace_id, w.name, w.description, w.created_at, w.updated_at, 
                   w.is_active, wm.role as member_role, wm.created_at as joined_at
            FROM workspaces w
            JOIN workspace_members wm ON w.workspace_id = wm.workspace_id
            WHERE wm.user_id = $1 {active_filter}
            ORDER BY wm.created_at ASC
        """
        
        workspaces = await self.db_manager.execute_query(
            query, (user_id_obj,), fetch_all=True
        )
        return [dict(ws) for ws in workspaces] if workspaces else []
    
    async def get_workspace_members(
        self, 
        workspace_id: Union[str, UUID],
        role_filter: Optional[str] = None
    ) -> List[Dict]:
        """Get all members of a workspace"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        role_condition = ""
        params = [workspace_id_obj]
        
        if role_filter:
            role_condition = "AND wm.role = $2"
            params.append(role_filter)
        
        query = f"""
            SELECT u.user_id, u.username, u.email, u.role as system_role, 
                   wm.role as workspace_role, wm.created_at as joined_at,
                   u.is_active, u.last_login
            FROM workspace_members wm
            JOIN users u ON wm.user_id = u.user_id
            WHERE wm.workspace_id = $1 {role_condition}
            ORDER BY wm.created_at ASC
        """
        
        members = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
        return [dict(member) for member in members] if members else []
    
    async def check_workspace_membership(
        self, 
        user_id: Union[str, UUID], 
        workspace_id: Union[str, UUID]
    ) -> Optional[str]:
        """Check if user is member of workspace and return their role"""
        query = """
            SELECT role FROM workspace_members 
            WHERE user_id = $1 AND workspace_id = $2
        """
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        result = await self.db_manager.execute_query(
            query, (user_id_obj, workspace_id_obj), fetch_one=True
        )
        return result['role'] if result else None
    
    async def create_workspace(
        self,
        name: str,
        description: Optional[str] = None,
        created_by_user_id: Optional[Union[str, UUID]] = None
    ) -> UUID:
        """Create a new workspace and optionally add creator as owner"""
        workspace_id = uuid4()
        now = datetime.now(timezone.utc)
        
        async with self.db_manager.transaction() as conn:
            # Create workspace
            ws_query = """
                INSERT INTO workspaces 
                (workspace_id, name, description, created_at, updated_at, is_active)
                VALUES ($1, $2, $3, $4, $5, $6)
            """
            await self.db_manager.execute_query(
                ws_query,
                (workspace_id, name, description, now, now, True),
                connection=conn
            )
            
            # Add creator as owner if provided
            if created_by_user_id:
                user_id_obj = (created_by_user_id if isinstance(created_by_user_id, UUID) 
                              else UUID(str(created_by_user_id)))
                
                membership_query = """
                    INSERT INTO workspace_members
                    (membership_id, workspace_id, user_id, role, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """
                await self.db_manager.execute_query(
                    membership_query,
                    (uuid4(), workspace_id, user_id_obj, 'owner', now, now),
                    connection=conn
                )
        
        return workspace_id
    
    async def add_workspace_member(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        role: str = 'member'
    ) -> bool:
        """Add a user to a workspace"""
        valid_roles = ['member', 'admin', 'owner']
        if role not in valid_roles:
            raise ValueError(f"Invalid role. Must be one of: {valid_roles}")
        
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO workspace_members
            (membership_id, workspace_id, user_id, role, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (workspace_id, user_id) DO NOTHING
        """
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (uuid4(), workspace_id_obj, user_id_obj, role, now, now),
            return_rowcount=True
        )
        return rows_affected > 0
    
    async def update_workspace_member_role(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        new_role: str
    ) -> bool:
        """Update a workspace member's role"""
        valid_roles = ['member', 'admin', 'owner']
        if new_role not in valid_roles:
            raise ValueError(f"Invalid role. Must be one of: {valid_roles}")
        
        query = """
            UPDATE workspace_members 
            SET role = $1, updated_at = $2
            WHERE workspace_id = $3 AND user_id = $4
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (new_role, datetime.now(timezone.utc), workspace_id_obj, user_id_obj),
            return_rowcount=True
        )
        return rows_affected > 0
    
    # ========== Video Stream / Camera Operations ==========
    
    async def get_stream_by_id(self, stream_id: Union[str, UUID]) -> Optional[Dict]:
        """Get video stream details"""
        query = """
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   vs.latitude, vs.longitude, vs.workspace_id, vs.user_id,
                   vs.created_at, vs.updated_at,
                   u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE vs.stream_id = $1
        """
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        stream = await self.db_manager.execute_query(
            query, (stream_id_obj,), fetch_one=True
        )
        return dict(stream) if stream else None
    
    async def get_workspace_streams(
        self,
        workspace_id: Union[str, UUID],
        user_id: Optional[Union[str, UUID]] = None,
        status_filter: Optional[str] = None,
        include_inactive: bool = True
    ) -> List[Dict]:
        """Get all streams in a workspace"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        conditions = ["vs.workspace_id = $1"]
        params = [workspace_id_obj]
        
        if user_id:
            user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
            conditions.append(f"vs.user_id = ${len(params) + 1}")
            params.append(user_id_obj)
        
        if status_filter:
            conditions.append(f"vs.status = ${len(params) + 1}")
            params.append(status_filter)
        elif not include_inactive:
            conditions.append("vs.status != 'inactive'")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT vs.stream_id, vs.name, vs.path, vs.type, vs.status, vs.is_streaming,
                   vs.location, vs.area, vs.building, vs.zone, vs.floor_level,
                   vs.latitude, vs.longitude, vs.workspace_id, vs.user_id,
                   vs.created_at, vs.updated_at,
                   u.username as owner_username
            FROM video_stream vs
            JOIN users u ON vs.user_id = u.user_id
            WHERE {where_clause}
            ORDER BY vs.created_at DESC
        """
        
        streams = await self.db_manager.execute_query(query, tuple(params), fetch_all=True)
        return [dict(stream) for stream in streams] if streams else []
    
    async def create_stream(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        name: str,
        path: str,
        stream_type: str = 'rtsp',
        location_info: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Create a new video stream"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        stream_id = uuid4()
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO video_stream
            (stream_id, workspace_id, user_id, name, path, type, status, is_streaming,
             location, area, building, zone, floor_level, latitude, longitude,
             created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """
        
        location = location_info.get('location') if location_info else None
        area = location_info.get('area') if location_info else None
        building = location_info.get('building') if location_info else None
        zone = location_info.get('zone') if location_info else None
        floor_level = location_info.get('floor_level') if location_info else None
        latitude = float(location_info['latitude']) if location_info and location_info.get('latitude') else None
        longitude = float(location_info['longitude']) if location_info and location_info.get('longitude') else None
        
        await self.db_manager.execute_query(
            query,
            (stream_id, workspace_id_obj, user_id_obj, name, path, stream_type, 
             'inactive', False, location, area, building, zone, floor_level,
             latitude, longitude, now, now)
        )
        
        return stream_id
    
    async def update_stream(
        self,
        stream_id: Union[str, UUID],
        updates: StreamUpdate
    ) -> bool:
        """Update stream properties"""
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        # Build dynamic update query
        update_fields = []
        params = []
        param_count = 1
        
        update_dict = updates.model_dump(exclude_none=True)
        
        for field, value in update_dict.items():
            if field in ['latitude', 'longitude'] and value is not None:
                value = float(value)
            update_fields.append(f"{field} = ${param_count}")
            params.append(value)
            param_count += 1
        
        if not update_fields:
            return False
        
        # Always update updated_at
        update_fields.append(f"updated_at = ${param_count}")
        params.append(datetime.now(timezone.utc))
        param_count += 1
        
        params.append(stream_id_obj)
        
        query = f"""
            UPDATE video_stream
            SET {', '.join(update_fields)}
            WHERE stream_id = ${param_count}
        """
        
        rows_affected = await self.db_manager.execute_query(
            query, tuple(params), return_rowcount=True
        )
        return rows_affected > 0
    
    async def update_stream_status(
        self,
        stream_id: Union[str, UUID],
        status: str,
        is_streaming: Optional[bool] = None
    ) -> bool:
        """Update stream status and streaming state"""
        valid_statuses = ['active', 'inactive', 'error', 'pending']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {valid_statuses}")
        
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        if is_streaming is not None:
            query = """
                UPDATE video_stream
                SET status = $1, is_streaming = $2, updated_at = $3
                WHERE stream_id = $4
            """
            params = (status, is_streaming, datetime.now(timezone.utc), stream_id_obj)
        else:
            query = """
                UPDATE video_stream
                SET status = $1, updated_at = $2
                WHERE stream_id = $3
            """
            params = (status, datetime.now(timezone.utc), stream_id_obj)
        
        rows_affected = await self.db_manager.execute_query(
            query, params, return_rowcount=True
        )
        return rows_affected > 0
    
    async def delete_stream(self, stream_id: Union[str, UUID]) -> bool:
        """Delete a video stream"""
        query = "DELETE FROM video_stream WHERE stream_id = $1"
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (stream_id_obj,), return_rowcount=True
        )
        return rows_affected > 0
    
    async def get_user_stream_count(self, user_id: Union[str, UUID]) -> int:
        """Get count of streams owned by user"""
        query = "SELECT COUNT(*) as count FROM video_stream WHERE user_id = $1"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        result = await self.db_manager.execute_query(
            query, (user_id_obj,), fetch_one=True
        )
        return result['count'] if result else 0
    
    # ========== Session Operations ==========
    
    async def create_session(
        self,
        user_id: Union[str, UUID],
        workspace_id: Union[str, UUID],
        session_token: str,
        expires_at: datetime,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> UUID:
        """Create a new user session"""
        session_id = uuid4()
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO sessions
            (session_id, user_id, workspace_id, session_token, expires_at, 
             ip_address, user_agent, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        
        await self.db_manager.execute_query(
            query,
            (session_id, user_id_obj, workspace_id_obj, session_token, expires_at,
             ip_address, user_agent, now, now)
        )
        
        return session_id
    
    async def get_session_by_token(self, session_token: str) -> Optional[Dict]:
        """Get session by token"""
        query = """
            SELECT session_id, user_id, workspace_id, session_token, expires_at,
                   ip_address, user_agent, created_at, updated_at
            FROM sessions
            WHERE session_token = $1 AND expires_at > $2
        """
        
        session = await self.db_manager.execute_query(
            query, (session_token, datetime.now(timezone.utc)), fetch_one=True
        )
        return dict(session) if session else None
    
    async def delete_session(self, session_id: Union[str, UUID]) -> bool:
        """Delete a session"""
        query = "DELETE FROM sessions WHERE session_id = $1"
        session_id_obj = session_id if isinstance(session_id, UUID) else UUID(str(session_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (session_id_obj,), return_rowcount=True
        )
        return rows_affected > 0
    
    async def delete_user_sessions(self, user_id: Union[str, UUID]) -> int:
        """Delete all sessions for a user"""
        query = "DELETE FROM sessions WHERE user_id = $1"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (user_id_obj,), return_rowcount=True
        )
        return rows_affected
    
    async def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions"""
        query = "DELETE FROM sessions WHERE expires_at < $1"
        
        rows_affected = await self.db_manager.execute_query(
            query, (datetime.now(timezone.utc),), return_rowcount=True
        )
        
        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} expired sessions")
        
        return rows_affected
    
    # ========== Token Operations ==========
    
    async def store_token(
        self,
        user_id: Union[str, UUID],
        workspace_id: Union[str, UUID],
        access_token: str,
        refresh_token: str,
        access_expires_at: datetime,
        refresh_expires_at: datetime
    ) -> UUID:
        """Store access and refresh tokens"""
        token_id = uuid4()
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO user_tokens
            (token_id, user_id, workspace_id, access_token, refresh_token,
             access_expires_at, refresh_expires_at, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """
        
        await self.db_manager.execute_query(
            query,
            (token_id, user_id_obj, workspace_id_obj, access_token, refresh_token,
             access_expires_at, refresh_expires_at, True, now, now)
        )
        
        return token_id
    
    async def get_token_by_access(self, access_token: str) -> Optional[Dict]:
        """Get token info by access token"""
        query = """
            SELECT token_id, user_id, workspace_id, access_token, refresh_token,
                   access_expires_at, refresh_expires_at, is_active, created_at, updated_at
            FROM user_tokens
            WHERE access_token = $1 AND is_active = TRUE
        """
        
        token = await self.db_manager.execute_query(
            query, (access_token,), fetch_one=True
        )
        return dict(token) if token else None
    
    async def get_token_by_refresh(self, refresh_token: str) -> Optional[Dict]:
        """Get token info by refresh token"""
        query = """
            SELECT token_id, user_id, workspace_id, access_token, refresh_token,
                   access_expires_at, refresh_expires_at, is_active, created_at, updated_at
            FROM user_tokens
            WHERE refresh_token = $1 AND is_active = TRUE
        """
        
        token = await self.db_manager.execute_query(
            query, (refresh_token,), fetch_one=True
        )
        return dict(token) if token else None
    
    async def revoke_token(self, token_id: Union[str, UUID]) -> bool:
        """Revoke a token by setting is_active to False"""
        query = "UPDATE user_tokens SET is_active = FALSE, updated_at = $1 WHERE token_id = $2"
        token_id_obj = token_id if isinstance(token_id, UUID) else UUID(str(token_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (datetime.now(timezone.utc), token_id_obj), return_rowcount=True
        )
        return rows_affected > 0
    
    async def revoke_user_tokens(self, user_id: Union[str, UUID]) -> int:
        """Revoke all tokens for a user"""
        query = "UPDATE user_tokens SET is_active = FALSE, updated_at = $1 WHERE user_id = $2"
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (datetime.now(timezone.utc), user_id_obj), return_rowcount=True
        )
        return rows_affected
    
    async def cleanup_expired_tokens(self) -> int:
        """Remove expired tokens"""
        query = """
            DELETE FROM user_tokens 
            WHERE refresh_expires_at < $1 OR (is_active = FALSE AND updated_at < $2)
        """
        now = datetime.now(timezone.utc)
        cleanup_threshold = now - timedelta(days=7)  # Remove inactive tokens older than 7 days
        
        rows_affected = await self.db_manager.execute_query(
            query, (now, cleanup_threshold), return_rowcount=True
        )
        
        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} expired/inactive tokens")
        
        return rows_affected
    
    # ========== Notification Operations ==========
    
    async def create_notification(
        self,
        user_id: Union[str, UUID],
        workspace_id: Union[str, UUID],
        title: str,
        message: str,
        notification_type: str = 'info',
        stream_id: Optional[Union[str, UUID]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Create a notification"""
        import json
        
        notification_id = uuid4()
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        stream_id_obj = (stream_id if isinstance(stream_id, UUID) 
                        else UUID(str(stream_id))) if stream_id else None
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO notifications
            (notification_id, user_id, workspace_id, stream_id, title, message, 
             type, metadata, is_read, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        await self.db_manager.execute_query(
            query,
            (notification_id, user_id_obj, workspace_id_obj, stream_id_obj, 
             title, message, notification_type, metadata_json, False, now)
        )
        
        return notification_id
    
    async def get_user_notifications(
        self,
        user_id: Union[str, UUID],
        workspace_id: Optional[Union[str, UUID]] = None,
        unread_only: bool = False,
        limit: int = 50
    ) -> List[Dict]:
        """Get notifications for a user"""
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        conditions = ["user_id = $1"]
        params = [user_id_obj]
        
        if workspace_id:
            workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
            conditions.append(f"workspace_id = ${len(params) + 1}")
            params.append(workspace_id_obj)
        
        if unread_only:
            conditions.append("is_read = FALSE")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT notification_id, user_id, workspace_id, stream_id, title, message,
                   type, metadata, is_read, created_at
            FROM notifications
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1}
        """
        params.append(limit)
        
        notifications = await self.db_manager.execute_query(
            query, tuple(params), fetch_all=True
        )
        return [dict(notif) for notif in notifications] if notifications else []
    
    async def mark_notification_read(
        self,
        notification_id: Union[str, UUID],
        user_id: Optional[Union[str, UUID]] = None
    ) -> bool:
        """Mark a notification as read"""
        notification_id_obj = notification_id if isinstance(notification_id, UUID) else UUID(str(notification_id))
        
        if user_id:
            user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
            query = """
                UPDATE notifications 
                SET is_read = TRUE 
                WHERE notification_id = $1 AND user_id = $2
            """
            params = (notification_id_obj, user_id_obj)
        else:
            query = "UPDATE notifications SET is_read = TRUE WHERE notification_id = $1"
            params = (notification_id_obj,)
        
        rows_affected = await self.db_manager.execute_query(
            query, params, return_rowcount=True
        )
        return rows_affected > 0
    
    async def mark_all_notifications_read(
        self,
        user_id: Union[str, UUID],
        workspace_id: Optional[Union[str, UUID]] = None
    ) -> int:
        """Mark all notifications as read for a user"""
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        if workspace_id:
            workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
            query = """
                UPDATE notifications 
                SET is_read = TRUE 
                WHERE user_id = $1 AND workspace_id = $2 AND is_read = FALSE
            """
            params = (user_id_obj, workspace_id_obj)
        else:
            query = """
                UPDATE notifications 
                SET is_read = TRUE 
                WHERE user_id = $1 AND is_read = FALSE
            """
            params = (user_id_obj,)
        
        rows_affected = await self.db_manager.execute_query(
            query, params, return_rowcount=True
        )
        return rows_affected
    
    async def delete_notification(self, notification_id: Union[str, UUID]) -> bool:
        """Delete a notification"""
        query = "DELETE FROM notifications WHERE notification_id = $1"
        notification_id_obj = notification_id if isinstance(notification_id, UUID) else UUID(str(notification_id))
        
        rows_affected = await self.db_manager.execute_query(
            query, (notification_id_obj,), return_rowcount=True
        )
        return rows_affected > 0
    
    async def cleanup_old_notifications(self, days: int = 30) -> int:
        """Delete notifications older than specified days"""
        query = "DELETE FROM notifications WHERE created_at < $1"
        threshold = datetime.now(timezone.utc) - timedelta(days=days)
        
        rows_affected = await self.db_manager.execute_query(
            query, (threshold,), return_rowcount=True
        )
        
        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} old notifications")
        
        return rows_affected
    
    # ========== Security Event Logging ==========
    
    async def log_security_event(
        self,
        user_id: Optional[Union[str, UUID]],
        event_type: str,
        severity: str,
        event_data: Dict[str, Any],
        ip_address: Optional[str] = None,
        workspace_id: Optional[Union[str, UUID]] = None
    ) -> UUID:
        """Log a security event"""
        import json
        
        event_id = uuid4()
        user_id_obj = (user_id if isinstance(user_id, UUID) 
                      else UUID(str(user_id))) if user_id else None
        workspace_id_obj = (workspace_id if isinstance(workspace_id, UUID) 
                           else UUID(str(workspace_id))) if workspace_id else None
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO security_events
            (event_id, user_id, workspace_id, event_type, severity, 
             ip_address, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        
        await self.db_manager.execute_query(
            query,
            (event_id, user_id_obj, workspace_id_obj, event_type, severity,
             ip_address, json.dumps(event_data), now)
        )
        
        return event_id
    
    async def get_security_events(
        self,
        user_id: Optional[Union[str, UUID]] = None,
        workspace_id: Optional[Union[str, UUID]] = None,
        event_type: Optional[str] = None,
        severity: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get security events with filters"""
        conditions = []
        params = []
        
        if user_id:
            user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
            conditions.append(f"user_id = ${len(params) + 1}")
            params.append(user_id_obj)
        
        if workspace_id:
            workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
            conditions.append(f"workspace_id = ${len(params) + 1}")
            params.append(workspace_id_obj)
        
        if event_type:
            conditions.append(f"event_type = ${len(params) + 1}")
            params.append(event_type)
        
        if severity:
            conditions.append(f"severity = ${len(params) + 1}")
            params.append(severity)
        
        if start_date:
            conditions.append(f"created_at >= ${len(params) + 1}")
            params.append(start_date)
        
        if end_date:
            conditions.append(f"created_at <= ${len(params) + 1}")
            params.append(end_date)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT event_id, user_id, workspace_id, event_type, severity,
                   ip_address, event_data, created_at
            FROM security_events
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1}
        """
        params.append(limit)
        
        events = await self.db_manager.execute_query(
            query, tuple(params), fetch_all=True
        )
        return [dict(event) for event in events] if events else []
    
    # ========== Activity Logs ==========
    
    async def create_log(
        self,
        user_id: Optional[Union[str, UUID]],
        workspace_id: Optional[Union[str, UUID]],
        action_type: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None
    ) -> UUID:
        """Create an activity log entry"""
        import json
        
        log_id = uuid4()
        user_id_obj = (user_id if isinstance(user_id, UUID) 
                      else UUID(str(user_id))) if user_id else None
        workspace_id_obj = (workspace_id if isinstance(workspace_id, UUID) 
                           else UUID(str(workspace_id))) if workspace_id else None
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO logs
            (log_id, user_id, workspace_id, action_type, status, 
             details, ip_address, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        
        details_json = json.dumps(details) if details else None
        
        await self.db_manager.execute_query(
            query,
            (log_id, user_id_obj, workspace_id_obj, action_type, status,
             details_json, ip_address, now)
        )
        
        return log_id
    
    async def get_logs(
        self,
        user_id: Optional[Union[str, UUID]] = None,
        workspace_id: Optional[Union[str, UUID]] = None,
        action_type: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get activity logs with filters"""
        conditions = []
        params = []
        
        if user_id:
            user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
            conditions.append(f"user_id = ${len(params) + 1}")
            params.append(user_id_obj)
        
        if workspace_id:
            workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
            conditions.append(f"workspace_id = ${len(params) + 1}")
            params.append(workspace_id_obj)
        
        if action_type:
            conditions.append(f"action_type = ${len(params) + 1}")
            params.append(action_type)
        
        if status:
            conditions.append(f"status = ${len(params) + 1}")
            params.append(status)
        
        if start_date:
            conditions.append(f"created_at >= ${len(params) + 1}")
            params.append(start_date)
        
        if end_date:
            conditions.append(f"created_at <= ${len(params) + 1}")
            params.append(end_date)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT log_id, user_id, workspace_id, action_type, status,
                   details, ip_address, created_at
            FROM logs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1}
        """
        params.append(limit)
        
        logs = await self.db_manager.execute_query(
            query, tuple(params), fetch_all=True
        )
        return [dict(log) for log in logs] if logs else []
    
    # ========== OTP Operations ==========
    
    async def create_otp(
        self,
        email: str,
        otp_code: str,
        purpose: str,
        expires_at: datetime
    ) -> UUID:
        """Create an OTP record"""
        otp_id = uuid4()
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO otps
            (otp_id, email, otp_code, purpose, expires_at, is_used, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        
        await self.db_manager.execute_query(
            query,
            (otp_id, email, otp_code, purpose, expires_at, False, now)
        )
        
        return otp_id
    
    async def verify_otp(
        self,
        email: str,
        otp_code: str,
        purpose: str
    ) -> bool:
        """Verify an OTP code"""
        query = """
            SELECT otp_id FROM otps
            WHERE email = $1 AND otp_code = $2 AND purpose = $3 
                  AND expires_at > $4 AND is_used = FALSE
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        result = await self.db_manager.execute_query(
            query,
            (email, otp_code, purpose, datetime.now(timezone.utc)),
            fetch_one=True
        )
        
        if result:
            # Mark OTP as used
            update_query = "UPDATE otps SET is_used = TRUE WHERE otp_id = $1"
            await self.db_manager.execute_query(update_query, (result['otp_id'],))
            return True
        
        return False
    
    async def cleanup_expired_otps(self) -> int:
        """Remove expired OTPs"""
        query = "DELETE FROM otps WHERE expires_at < $1 OR is_used = TRUE"
        
        rows_affected = await self.db_manager.execute_query(
            query, (datetime.now(timezone.utc),), return_rowcount=True
        )
        
        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} expired/used OTPs")
        
        return rows_affected
    
    # ========== Token Blacklist Operations ==========
    
    async def blacklist_token(
        self,
        token: str,
        user_id: Union[str, UUID],
        expires_at: datetime,
        reason: Optional[str] = None
    ) -> UUID:
        """Add a token to the blacklist"""
        blacklist_id = uuid4()
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO token_blacklist
            (blacklist_id, token, user_id, expires_at, reason, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        
        await self.db_manager.execute_query(
            query,
            (blacklist_id, token, user_id_obj, expires_at, reason, now)
        )
        
        return blacklist_id
    
    async def is_token_blacklisted(self, token: str) -> bool:
        """Check if a token is blacklisted"""
        query = """
            SELECT blacklist_id FROM token_blacklist
            WHERE token = $1 AND expires_at > $2
        """
        
        result = await self.db_manager.execute_query(
            query, (token, datetime.now(timezone.utc)), fetch_one=True
        )
        
        return result is not None
    
    async def cleanup_expired_blacklist(self) -> int:
        """Remove expired blacklisted tokens"""
        query = "DELETE FROM token_blacklist WHERE expires_at < $1"
        
        rows_affected = await self.db_manager.execute_query(
            query, (datetime.now(timezone.utc),), return_rowcount=True
        )
        
        if rows_affected > 0:
            logger.info(f"Cleaned up {rows_affected} expired blacklisted tokens")
        
        return rows_affected
    
    # ========== Parameter Stream Operations ==========
    
    async def create_param_stream(
        self,
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        parameters: Dict[str, Any]
    ) -> UUID:
        """Create a parameter stream entry"""
        import json
        
        param_id = uuid4()
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO param_stream
            (param_id, workspace_id, user_id, parameters, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        
        await self.db_manager.execute_query(
            query,
            (param_id, workspace_id_obj, user_id_obj, json.dumps(parameters), now, now)
        )
        
        return param_id
    
    async def get_param_stream(
        self,
        workspace_id: Union[str, UUID]
    ) -> Optional[Dict]:
        """Get parameter stream for workspace"""
        query = """
            SELECT param_id, workspace_id, user_id, parameters, created_at, updated_at
            FROM param_stream
            WHERE workspace_id = $1
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        result = await self.db_manager.execute_query(
            query, (workspace_id_obj,), fetch_one=True
        )
        return dict(result) if result else None
    
    async def update_param_stream(
        self,
        workspace_id: Union[str, UUID],
        parameters: Dict[str, Any]
    ) -> bool:
        """Update parameter stream"""
        import json
        
        query = """
            UPDATE param_stream
            SET parameters = $1, updated_at = $2
            WHERE workspace_id = $3
        """
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (json.dumps(parameters), datetime.now(timezone.utc), workspace_id_obj),
            return_rowcount=True
        )
        return rows_affected > 0
    
    # ========== Fire Detection State Operations ==========
    
    async def get_fire_detection_state(
        self,
        stream_id: Union[str, UUID]
    ) -> Optional[Dict]:
        """Get fire detection state for a stream"""
        query = """
            SELECT stream_id, fire_status, last_detection_time, 
                   last_notification_time, created_at, updated_at
            FROM fire_detection_state
            WHERE stream_id = $1
        """
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        
        result = await self.db_manager.execute_query(
            query, (stream_id_obj,), fetch_one=True
        )
        return dict(result) if result else None
    
    async def upsert_fire_detection_state(
        self,
        stream_id: Union[str, UUID],
        fire_status: str,
        last_detection_time: Optional[datetime] = None,
        last_notification_time: Optional[datetime] = None
    ) -> bool:
        """Create or update fire detection state"""
        stream_id_obj = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        now = datetime.now(timezone.utc)
        detection_time = last_detection_time or now
        
        query = """
            INSERT INTO fire_detection_state
            (stream_id, fire_status, last_detection_time, last_notification_time, 
             created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (stream_id) DO UPDATE SET
                fire_status = EXCLUDED.fire_status,
                last_detection_time = EXCLUDED.last_detection_time,
                last_notification_time = CASE 
                    WHEN EXCLUDED.last_notification_time IS NOT NULL 
                    THEN EXCLUDED.last_notification_time 
                    ELSE fire_detection_state.last_notification_time 
                END,
                updated_at = EXCLUDED.updated_at
        """
        
        rows_affected = await self.db_manager.execute_query(
            query,
            (stream_id_obj, fire_status, detection_time, last_notification_time, now, now),
            return_rowcount=True
        )
        return rows_affected > 0
    
    async def cleanup_old_fire_states(self) -> int:
        """Clean up old fire detection states"""
        # Remove states for deleted streams
        query1 = """
            DELETE FROM fire_detection_state 
            WHERE stream_id NOT IN (SELECT stream_id FROM video_stream)
        """
        rows1 = await self.db_manager.execute_query(query1, return_rowcount=True)
        
        # Clear old notification times (older than 24 hours)
        day_ago = datetime.now(timezone.utc) - timedelta(days=1)
        query2 = """
            UPDATE fire_detection_state 
            SET last_notification_time = NULL 
            WHERE last_notification_time < $1
        """
        rows2 = await self.db_manager.execute_query(query2, (day_ago,), return_rowcount=True)
        
        total_affected = rows1 + rows2
        if total_affected > 0:
            logger.info(f"Cleaned up {total_affected} fire detection state records")
        
        return total_affected
    
    # ========== Statistics and Analytics ==========
    
    async def get_workspace_statistics(
        self,
        workspace_id: Union[str, UUID]
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a workspace"""
        workspace_id_obj = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        
        stats = {
            'workspace_id': str(workspace_id_obj),
            'members': {'total': 0, 'by_role': {}},
            'streams': {'total': 0, 'active': 0, 'streaming': 0, 'by_status': {}},
            'notifications': {'total': 0, 'unread': 0},
            'recent_activity': []
        }
        
        # Member counts
        member_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE role = 'owner') as owners,
                   COUNT(*) FILTER (WHERE role = 'admin') as admins,
                   COUNT(*) FILTER (WHERE role = 'member') as members
            FROM workspace_members
            WHERE workspace_id = $1
        """
        member_stats = await self.db_manager.execute_query(
            member_query, (workspace_id_obj,), fetch_one=True
        )
        
        if member_stats:
            stats['members']['total'] = member_stats['total']
            stats['members']['by_role'] = {
                'owner': member_stats['owners'],
                'admin': member_stats['admins'],
                'member': member_stats['members']
            }
        
        # Stream counts
        stream_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE status = 'active') as active,
                   COUNT(*) FILTER (WHERE is_streaming = TRUE) as streaming,
                   COUNT(*) FILTER (WHERE status = 'inactive') as inactive,
                   COUNT(*) FILTER (WHERE status = 'error') as error
            FROM video_stream
            WHERE workspace_id = $1
        """
        stream_stats = await self.db_manager.execute_query(
            stream_query, (workspace_id_obj,), fetch_one=True
        )
        
        if stream_stats:
            stats['streams']['total'] = stream_stats['total']
            stats['streams']['active'] = stream_stats['active']
            stats['streams']['streaming'] = stream_stats['streaming']
            stats['streams']['by_status'] = {
                'active': stream_stats['active'],
                'inactive': stream_stats['inactive'],
                'error': stream_stats['error']
            }
        
        # Notification counts
        notif_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE is_read = FALSE) as unread
            FROM notifications
            WHERE workspace_id = $1
        """
        notif_stats = await self.db_manager.execute_query(
            notif_query, (workspace_id_obj,), fetch_one=True
        )
        
        if notif_stats:
            stats['notifications']['total'] = notif_stats['total']
            stats['notifications']['unread'] = notif_stats['unread']
        
        return stats
    
    async def get_user_statistics(
        self,
        user_id: Union[str, UUID]
    ) -> Dict[str, Any]:
        """Get comprehensive statistics for a user"""
        user_id_obj = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
        
        stats = {
            'user_id': str(user_id_obj),
            'workspaces': {'total': 0, 'by_role': {}},
            'streams': {'total': 0, 'active': 0, 'streaming': 0},
            'notifications': {'total': 0, 'unread': 0},
            'sessions': {'active': 0}
        }
        
        # Workspace counts
        ws_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE role = 'owner') as owner,
                   COUNT(*) FILTER (WHERE role = 'admin') as admin,
                   COUNT(*) FILTER (WHERE role = 'member') as member
            FROM workspace_members
            WHERE user_id = $1
        """
        ws_stats = await self.db_manager.execute_query(
            ws_query, (user_id_obj,), fetch_one=True
        )
        
        if ws_stats:
            stats['workspaces']['total'] = ws_stats['total']
            stats['workspaces']['by_role'] = {
                'owner': ws_stats['owner'],
                'admin': ws_stats['admin'],
                'member': ws_stats['member']
            }
        
        # Stream counts
        stream_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE status = 'active') as active,
                   COUNT(*) FILTER (WHERE is_streaming = TRUE) as streaming
            FROM video_stream
            WHERE user_id = $1
        """
        stream_stats = await self.db_manager.execute_query(
            stream_query, (user_id_obj,), fetch_one=True
        )
        
        if stream_stats:
            stats['streams'] = dict(stream_stats)
        
        # Notification counts
        notif_query = """
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE is_read = FALSE) as unread
            FROM notifications
            WHERE user_id = $1
        """
        notif_stats = await self.db_manager.execute_query(
            notif_query, (user_id_obj,), fetch_one=True
        )
        
        if notif_stats:
            stats['notifications'] = dict(notif_stats)
        
        # Active sessions
        session_query = """
            SELECT COUNT(*) as active
            FROM sessions
            WHERE user_id = $1 AND expires_at > $2
        """
        session_stats = await self.db_manager.execute_query(
            session_query, (user_id_obj, datetime.now(timezone.utc)), fetch_one=True
        )
        
        if session_stats:
            stats['sessions']['active'] = session_stats['active']
        
        return stats
    
    # ========== Cleanup and Maintenance ==========
    
    async def perform_maintenance(self) -> Dict[str, int]:
        """Perform all maintenance tasks"""
        results = {
            'expired_sessions': 0,
            'expired_tokens': 0,
            'expired_otps': 0,
            'expired_blacklist': 0,
            'old_notifications': 0,
            'old_fire_states': 0
        }
        
        try:
            results['expired_sessions'] = await self.cleanup_expired_sessions()
            results['expired_tokens'] = await self.cleanup_expired_tokens()
            results['expired_otps'] = await self.cleanup_expired_otps()
            results['expired_blacklist'] = await self.cleanup_expired_blacklist()
            results['old_notifications'] = await self.cleanup_old_notifications(days=30)
            results['old_fire_states'] = await self.cleanup_old_fire_states()
            
            logger.info(f"Database maintenance completed: {results}")
        except Exception as e:
            logger.error(f"Error during database maintenance: {e}", exc_info=True)
        
        return results
    
    # ========== Health Check ==========
    
    async def health_check(self) -> Dict[str, Any]:
        """Check database health and connectivity"""
        health = {
            'status': 'unknown',
            'database': 'disconnected',
            'pool': {},
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Simple query to check connection
            result = await self.db_manager.execute_query(
                "SELECT 1 as check", fetch_one=True
            )
            
            if result and result.get('check') == 1:
                health['database'] = 'connected'
                health['status'] = 'healthy'
            
            # Get pool stats if available
            if db_manager.connection_pool and not db_manager.connection_pool._closed:
                pool = db_manager.connection_pool
                health['pool'] = {
                    'size': pool.get_size(),
                    'min_size': pool.get_min_size(),
                    'max_size': pool.get_max_size(),
                    'free_size': pool.get_idle_size()
                }
        
        except Exception as e:
            health['status'] = 'unhealthy'
            health['error'] = str(e)
            logger.error(f"Database health check failed: {e}", exc_info=True)
        
        return health


# Global service instance
postgres_service = PostgresService()