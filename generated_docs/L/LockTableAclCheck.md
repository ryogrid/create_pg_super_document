# LockTableAclCheck

## Location
src/backend/commands/lockcmds.c: 280 - 299

## Overview
Checks whether the current user is permitted to acquire a specific lock mode on a table relation, implementing PostgreSQL's access control policy for table locking operations.

## Definition


## Detailed Description
 is a static function that enforces PostgreSQL's access control policies for table locking operations. The function determines the appropriate ACL (Access Control List) permissions required based on the requested lock mode and checks whether the specified user has sufficient privileges on the target relation.

The function implements a tiered permission model where different lock modes require different levels of access:
- **Any lock mode**: Permitted with MAINTAIN, UPDATE, DELETE, or TRUNCATE privileges
- **ACCESS SHARE and below**: Also permitted with SELECT privileges  
- **ROW EXCLUSIVE and below**: Also permitted with INSERT privileges

This design follows PostgreSQL's principle that users should have appropriate table-level privileges before being allowed to acquire locks that could affect other users' access to the table.

## Parameters / Member Variables
- : Object identifier of the relation (table) on which the lock is requested
- : The type of lock being requested (e.g., AccessShareLock, RowExclusiveLock, etc.)
- : Object identifier of the user requesting the lock

## Dependencies
- Functions called/Symbols referenced:
  - pg_class_aclcheck
  - ACL_MAINTAIN
  - ACL_UPDATE  
  - ACL_DELETE
  - ACL_TRUNCATE
  - ACL_SELECT
  - ACL_INSERT
  - AclResult
  - AclMode
- Called from (representative examples):
  - RangeVarCallbackForLockTable
  - LockViewRecurse_walker

## Notes and Other Information
- This is a static function within src/backend/commands/lockcmds.c, making it internal to the lock command implementation
- The function returns an AclResult indicating whether the access should be granted or denied
- The permission model is cumulative: higher-privilege operations (MAINTAIN, UPDATE, DELETE, TRUNCATE) automatically grant permission for any lock mode
- Lower-privilege operations (SELECT, INSERT) only grant permission for specific, less restrictive lock modes
- This function is part of PostgreSQL's comprehensive security model, ensuring that lock acquisition is subject to the same access control as the underlying table operations