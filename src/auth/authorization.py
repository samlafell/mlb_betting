"""
Authorization Service

Role-based access control (RBAC) system with hierarchical permissions,
dynamic permission checking, and comprehensive role management.
"""

from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
from uuid import UUID

from ..core.logging import get_logger, LogComponent
from ..data.database.connection import get_database_connection
from .models import Role, RoleCreate, RoleUpdate, UserRole, UserRoleCreate, User, PermissionCheck
from .exceptions import (
    AuthorizationError, InsufficientPermissionsError, RoleNotFoundError,
    UserNotFoundError
)

logger = get_logger(__name__, LogComponent.AUTH)


class AuthorizationService:
    """Role-based access control and permission management service."""
    
    def __init__(self):
        """Initialize authorization service."""
        self._permission_cache = {}
        self._role_hierarchy_cache = {}
    
    async def check_permission(self, user_id: int, permission: str) -> bool:
        """
        Check if user has specific permission.
        
        Args:
            user_id: User ID to check
            permission: Permission string to check (e.g., "data:read", "user:manage")
            
        Returns:
            True if user has permission
        """
        try:
            # Get user permissions from database function
            async with get_database_connection() as conn:
                result = await conn.fetchval(
                    "SELECT auth.user_has_permission($1, $2)",
                    user_id, permission
                )
                
            has_permission = bool(result)
            
            logger.debug(
                "Permission check completed",
                extra={
                    "user_id": user_id,
                    "permission": permission,
                    "has_permission": has_permission
                }
            )
            
            return has_permission
            
        except Exception as e:
            logger.error(
                "Permission check failed",
                error=e,
                extra={"user_id": user_id, "permission": permission}
            )
            return False
    
    async def check_multiple_permissions(
        self, user_id: int, permissions: List[str]
    ) -> Dict[str, bool]:
        """
        Check multiple permissions for a user.
        
        Args:
            user_id: User ID to check
            permissions: List of permission strings
            
        Returns:
            Dictionary mapping permissions to boolean results
        """
        results = {}
        
        for permission in permissions:
            results[permission] = await self.check_permission(user_id, permission)
        
        return results
    
    async def require_permission(self, user_id: int, permission: str) -> None:
        """
        Require user to have specific permission, raise exception if not.
        
        Args:
            user_id: User ID to check
            permission: Required permission
            
        Raises:
            InsufficientPermissionsError: If user lacks permission
        """
        if not await self.check_permission(user_id, permission):
            # Get user's current permissions for context
            user_permissions = await self.get_user_permissions(user_id)
            
            raise InsufficientPermissionsError(
                required_permission=permission,
                user_permissions=user_permissions
            )
    
    async def require_any_permission(self, user_id: int, permissions: List[str]) -> None:
        """
        Require user to have at least one of the specified permissions.
        
        Args:
            user_id: User ID to check
            permissions: List of acceptable permissions
            
        Raises:
            InsufficientPermissionsError: If user lacks all permissions
        """
        for permission in permissions:
            if await self.check_permission(user_id, permission):
                return
        
        # User doesn't have any of the required permissions
        user_permissions = await self.get_user_permissions(user_id)
        raise InsufficientPermissionsError(
            required_permission=f"Any of: {', '.join(permissions)}",
            user_permissions=user_permissions
        )
    
    async def require_all_permissions(self, user_id: int, permissions: List[str]) -> None:
        """
        Require user to have all specified permissions.
        
        Args:
            user_id: User ID to check
            permissions: List of required permissions
            
        Raises:
            InsufficientPermissionsError: If user lacks any permission
        """
        permission_results = await self.check_multiple_permissions(user_id, permissions)
        
        missing_permissions = [
            perm for perm, has_perm in permission_results.items() if not has_perm
        ]
        
        if missing_permissions:
            user_permissions = await self.get_user_permissions(user_id)
            raise InsufficientPermissionsError(
                required_permission=f"Missing: {', '.join(missing_permissions)}",
                user_permissions=user_permissions
            )
    
    async def get_user_permissions(self, user_id: int) -> List[str]:
        """
        Get all permissions for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            List of permission strings
        """
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                """
                SELECT all_permissions, roles
                FROM auth.user_permissions
                WHERE user_id = $1
                """,
                user_id
            )
            
            if not result or not result['all_permissions']:
                return []
            
            # Extract permissions from nested JSONB arrays
            all_permissions = set()
            for permission_array in result['all_permissions']:
                if isinstance(permission_array, list):
                    all_permissions.update(permission_array)
            
            return list(all_permissions)
    
    async def get_user_roles(self, user_id: int) -> List[Role]:
        """
        Get all active roles for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            List of Role objects
        """
        async with get_database_connection() as conn:
            results = await conn.fetch(
                """
                SELECT r.id, r.name, r.display_name, r.description, 
                       r.permissions, r.is_system_role, r.parent_role_id,
                       r.created_at, r.updated_at
                FROM auth.roles r
                JOIN auth.user_roles ur ON r.id = ur.role_id
                WHERE ur.user_id = $1 AND ur.is_active = true
                  AND (ur.effective_from IS NULL OR ur.effective_from <= NOW())
                  AND (ur.effective_until IS NULL OR ur.effective_until > NOW())
                ORDER BY r.name
                """,
                user_id
            )
            
            return [Role(**dict(row)) for row in results]
    
    # Role management methods
    async def create_role(self, role_data: RoleCreate) -> Role:
        """
        Create a new role.
        
        Args:
            role_data: Role creation data
            
        Returns:
            Created Role object
        """
        async with get_database_connection() as conn:
            # Check if role name already exists
            existing_role = await conn.fetchrow(
                "SELECT id FROM auth.roles WHERE name = $1",
                role_data.name
            )
            
            if existing_role:
                raise AuthorizationError(f"Role '{role_data.name}' already exists")
            
            # Validate parent role if specified
            if role_data.parent_role_id:
                parent_role = await conn.fetchrow(
                    "SELECT id FROM auth.roles WHERE id = $1",
                    role_data.parent_role_id
                )
                
                if not parent_role:
                    raise RoleNotFoundError(str(role_data.parent_role_id))
            
            # Create role
            result = await conn.fetchrow(
                """
                INSERT INTO auth.roles (
                    name, display_name, description, permissions, 
                    is_system_role, parent_role_id
                ) VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING *
                """,
                role_data.name,
                role_data.display_name,
                role_data.description,
                role_data.permissions,
                role_data.is_system_role,
                role_data.parent_role_id
            )
            
            logger.info(
                "Role created successfully",
                extra={
                    "role_id": result['id'],
                    "role_name": result['name'],
                    "permissions_count": len(role_data.permissions)
                }
            )
            
            return Role(**dict(result))
    
    async def update_role(self, role_id: int, role_data: RoleUpdate) -> Role:
        """
        Update an existing role.
        
        Args:
            role_id: Role ID to update
            role_data: Updated role data
            
        Returns:
            Updated Role object
        """
        async with get_database_connection() as conn:
            # Check if role exists and is not system role (unless allowing system role updates)
            existing_role = await conn.fetchrow(
                "SELECT * FROM auth.roles WHERE id = $1",
                role_id
            )
            
            if not existing_role:
                raise RoleNotFoundError(str(role_id))
            
            if existing_role['is_system_role']:
                raise AuthorizationError("Cannot modify system roles")
            
            # Validate parent role if specified
            if role_data.parent_role_id:
                parent_role = await conn.fetchrow(
                    "SELECT id FROM auth.roles WHERE id = $1",
                    role_data.parent_role_id
                )
                
                if not parent_role:
                    raise RoleNotFoundError(str(role_data.parent_role_id))
                
                # Prevent circular references
                if role_data.parent_role_id == role_id:
                    raise AuthorizationError("Role cannot be its own parent")
            
            # Build update query dynamically
            update_fields = []
            values = []
            param_count = 1
            
            if role_data.display_name is not None:
                update_fields.append(f"display_name = ${param_count}")
                values.append(role_data.display_name)
                param_count += 1
            
            if role_data.description is not None:
                update_fields.append(f"description = ${param_count}")
                values.append(role_data.description)
                param_count += 1
            
            if role_data.permissions is not None:
                update_fields.append(f"permissions = ${param_count}")
                values.append(role_data.permissions)
                param_count += 1
            
            if role_data.parent_role_id is not None:
                update_fields.append(f"parent_role_id = ${param_count}")
                values.append(role_data.parent_role_id)
                param_count += 1
            
            if not update_fields:
                # Return existing role if no updates
                return Role(**dict(existing_role))
            
            update_fields.append("updated_at = NOW()")
            values.append(role_id)
            
            query = f"""
                UPDATE auth.roles 
                SET {', '.join(update_fields)}
                WHERE id = ${param_count}
                RETURNING *
            """
            
            result = await conn.fetchrow(query, *values)
            
            logger.info(
                "Role updated successfully",
                extra={
                    "role_id": role_id,
                    "role_name": result['name'],
                    "updated_fields": len(update_fields) - 1
                }
            )
            
            return Role(**dict(result))
    
    async def delete_role(self, role_id: int) -> bool:
        """
        Delete a role (soft delete by marking as inactive).
        
        Args:
            role_id: Role ID to delete
            
        Returns:
            True if role was deleted
        """
        async with get_database_connection() as conn:
            # Check if role exists and is not system role
            existing_role = await conn.fetchrow(
                "SELECT name, is_system_role FROM auth.roles WHERE id = $1",
                role_id
            )
            
            if not existing_role:
                raise RoleNotFoundError(str(role_id))
            
            if existing_role['is_system_role']:
                raise AuthorizationError("Cannot delete system roles")
            
            # Check if role is assigned to any users
            user_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM auth.user_roles 
                WHERE role_id = $1 AND is_active = true
                """,
                role_id
            )
            
            if user_count > 0:
                raise AuthorizationError(
                    f"Cannot delete role assigned to {user_count} user(s). "
                    "Remove role assignments first."
                )
            
            # Delete the role
            await conn.execute(
                "DELETE FROM auth.roles WHERE id = $1",
                role_id
            )
            
            logger.info(
                "Role deleted successfully",
                extra={
                    "role_id": role_id,
                    "role_name": existing_role['name']
                }
            )
            
            return True
    
    async def get_role(self, role_id: int) -> Role:
        """
        Get role by ID.
        
        Args:
            role_id: Role ID
            
        Returns:
            Role object
        """
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT * FROM auth.roles WHERE id = $1",
                role_id
            )
            
            if not result:
                raise RoleNotFoundError(str(role_id))
            
            return Role(**dict(result))
    
    async def get_role_by_name(self, role_name: str) -> Role:
        """
        Get role by name.
        
        Args:
            role_name: Role name
            
        Returns:
            Role object
        """
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT * FROM auth.roles WHERE name = $1",
                role_name
            )
            
            if not result:
                raise RoleNotFoundError(role_name)
            
            return Role(**dict(result))
    
    async def list_roles(
        self,
        include_system_roles: bool = True,
        parent_role_id: Optional[int] = None
    ) -> List[Role]:
        """
        List all roles with optional filtering.
        
        Args:
            include_system_roles: Whether to include system roles
            parent_role_id: Filter by parent role ID
            
        Returns:
            List of Role objects
        """
        async with get_database_connection() as conn:
            query = "SELECT * FROM auth.roles WHERE 1=1"
            params = []
            param_count = 1
            
            if not include_system_roles:
                query += f" AND is_system_role = false"
            
            if parent_role_id is not None:
                query += f" AND parent_role_id = ${param_count}"
                params.append(parent_role_id)
                param_count += 1
            
            query += " ORDER BY name"
            
            results = await conn.fetch(query, *params)
            return [Role(**dict(row)) for row in results]
    
    # User-role assignment methods
    async def assign_role_to_user(
        self,
        user_id: int,
        role_id: int,
        assigned_by: Optional[int] = None,
        effective_from: Optional[datetime] = None,
        effective_until: Optional[datetime] = None
    ) -> UserRole:
        """
        Assign role to user.
        
        Args:
            user_id: User ID
            role_id: Role ID to assign
            assigned_by: ID of user making the assignment
            effective_from: When assignment becomes effective
            effective_until: When assignment expires
            
        Returns:
            UserRole object
        """
        async with get_database_connection() as conn:
            # Verify user and role exist
            user_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM auth.users WHERE id = $1)",
                user_id
            )
            
            if not user_exists:
                raise UserNotFoundError(str(user_id), "user_id")
            
            role_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM auth.roles WHERE id = $1)",
                role_id
            )
            
            if not role_exists:
                raise RoleNotFoundError(str(role_id))
            
            # Check if assignment already exists
            existing_assignment = await conn.fetchrow(
                """
                SELECT id FROM auth.user_roles 
                WHERE user_id = $1 AND role_id = $2 AND is_active = true
                """,
                user_id, role_id
            )
            
            if existing_assignment:
                logger.warning(
                    "Role already assigned to user",
                    extra={"user_id": user_id, "role_id": role_id}
                )
                return await self.get_user_role(existing_assignment['id'])
            
            # Create assignment
            result = await conn.fetchrow(
                """
                INSERT INTO auth.user_roles (
                    user_id, role_id, assigned_by, effective_from, effective_until
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                """,
                user_id, role_id, assigned_by, 
                effective_from or datetime.now(timezone.utc),
                effective_until
            )
            
            logger.info(
                "Role assigned to user",
                extra={
                    "user_id": user_id,
                    "role_id": role_id,
                    "assigned_by": assigned_by,
                    "assignment_id": result['id']
                }
            )
            
            return UserRole(**dict(result))
    
    async def remove_role_from_user(
        self,
        user_id: int,
        role_id: int,
        removed_by: Optional[int] = None
    ) -> bool:
        """
        Remove role from user.
        
        Args:
            user_id: User ID
            role_id: Role ID to remove
            removed_by: ID of user making the removal
            
        Returns:
            True if role was removed
        """
        async with get_database_connection() as conn:
            # Deactivate the assignment
            result = await conn.execute(
                """
                UPDATE auth.user_roles 
                SET is_active = false, effective_until = NOW()
                WHERE user_id = $1 AND role_id = $2 AND is_active = true
                """,
                user_id, role_id
            )
            
            if result == 'UPDATE 0':
                logger.warning(
                    "No active role assignment found to remove",
                    extra={"user_id": user_id, "role_id": role_id}
                )
                return False
            
            logger.info(
                "Role removed from user",
                extra={
                    "user_id": user_id,
                    "role_id": role_id,
                    "removed_by": removed_by
                }
            )
            
            return True
    
    async def get_user_role(self, user_role_id: int) -> UserRole:
        """
        Get user role assignment by ID.
        
        Args:
            user_role_id: User role assignment ID
            
        Returns:
            UserRole object
        """
        async with get_database_connection() as conn:
            result = await conn.fetchrow(
                "SELECT * FROM auth.user_roles WHERE id = $1",
                user_role_id
            )
            
            if not result:
                raise AuthorizationError(f"User role assignment {user_role_id} not found")
            
            return UserRole(**dict(result))
    
    async def list_user_role_assignments(
        self,
        user_id: Optional[int] = None,
        role_id: Optional[int] = None,
        active_only: bool = True
    ) -> List[UserRole]:
        """
        List user role assignments with optional filtering.
        
        Args:
            user_id: Filter by user ID
            role_id: Filter by role ID
            active_only: Only return active assignments
            
        Returns:
            List of UserRole objects
        """
        async with get_database_connection() as conn:
            query = "SELECT * FROM auth.user_roles WHERE 1=1"
            params = []
            param_count = 1
            
            if user_id is not None:
                query += f" AND user_id = ${param_count}"
                params.append(user_id)
                param_count += 1
            
            if role_id is not None:
                query += f" AND role_id = ${param_count}"
                params.append(role_id)
                param_count += 1
            
            if active_only:
                query += " AND is_active = true"
                query += " AND (effective_from IS NULL OR effective_from <= NOW())"
                query += " AND (effective_until IS NULL OR effective_until > NOW())"
            
            query += " ORDER BY assigned_at DESC"
            
            results = await conn.fetch(query, *params)
            return [UserRole(**dict(row)) for row in results]
    
    async def get_role_hierarchy(self, role_id: int) -> Dict[str, Any]:
        """
        Get complete role hierarchy for a role.
        
        Args:
            role_id: Root role ID
            
        Returns:
            Dictionary representing role hierarchy
        """
        async with get_database_connection() as conn:
            # Get all roles in hierarchy using recursive CTE
            results = await conn.fetch(
                """
                WITH RECURSIVE role_tree AS (
                    -- Base case: start with the requested role
                    SELECT id, name, display_name, permissions, parent_role_id, 0 as level
                    FROM auth.roles
                    WHERE id = $1
                    
                    UNION ALL
                    
                    -- Recursive case: get parent roles
                    SELECT r.id, r.name, r.display_name, r.permissions, r.parent_role_id, rt.level + 1
                    FROM auth.roles r
                    JOIN role_tree rt ON r.id = rt.parent_role_id
                    WHERE rt.level < 10  -- Prevent infinite recursion
                )
                SELECT * FROM role_tree ORDER BY level DESC
                """,
                role_id
            )
            
            if not results:
                raise RoleNotFoundError(str(role_id))
            
            # Build hierarchy structure
            hierarchy = {
                "role_id": role_id,
                "roles": [dict(row) for row in results],
                "combined_permissions": []
            }
            
            # Combine permissions from all roles in hierarchy
            all_permissions = set()
            for row in results:
                if row['permissions']:
                    all_permissions.update(row['permissions'])
            
            hierarchy["combined_permissions"] = list(all_permissions)
            
            return hierarchy
    
    async def validate_permission_format(self, permission: str) -> bool:
        """
        Validate permission string format.
        
        Args:
            permission: Permission string to validate
            
        Returns:
            True if format is valid
        """
        # Permission format: domain:action or domain:*
        # Examples: user:read, data:write, system:*
        
        if ':' not in permission:
            return False
        
        parts = permission.split(':')
        if len(parts) != 2:
            return False
        
        domain, action = parts
        
        # Validate domain (alphanumeric + underscore)
        if not domain or not domain.replace('_', '').isalnum():
            return False
        
        # Validate action (alphanumeric + underscore or wildcard)
        if not action or (action != '*' and not action.replace('_', '').isalnum()):
            return False
        
        return True
    
    async def expand_wildcard_permissions(self, permissions: List[str]) -> List[str]:
        """
        Expand wildcard permissions to specific permissions.
        
        Args:
            permissions: List of permissions that may contain wildcards
            
        Returns:
            List of expanded permissions
        """
        expanded_permissions = set()
        
        # Known permission domains and their actions
        permission_map = {
            "system": ["admin", "config", "monitor"],
            "user": ["create", "read", "update", "delete", "manage"],
            "data": ["read", "write", "delete", "export"],
            "analytics": ["read", "write", "export", "configure"],
            "ml": ["read", "write", "train", "deploy", "predict"],
            "monitoring": ["read", "configure", "alert"],
        }
        
        for permission in permissions:
            if permission.endswith(':*'):
                domain = permission[:-2]
                if domain in permission_map:
                    # Expand wildcard to all actions in domain
                    for action in permission_map[domain]:
                        expanded_permissions.add(f"{domain}:{action}")
                else:
                    # Keep wildcard if domain not in map
                    expanded_permissions.add(permission)
            elif permission == 'system:*':
                # Super admin - add all permissions
                for domain, actions in permission_map.items():
                    for action in actions:
                        expanded_permissions.add(f"{domain}:{action}")
            else:
                expanded_permissions.add(permission)
        
        return list(expanded_permissions)