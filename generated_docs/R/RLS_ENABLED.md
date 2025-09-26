# RLS_ENABLED

## Location
[src/include/utils/rls.h:45-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/rls.h#L45-L50)

## Overview
RLS_ENABLED is an enumeration value in the CheckEnableRlsResult enum that indicates Row Level Security (RLS) should be actively applied to queries on a table and that query plan invalidation is required when the security environment changes.

## Definition

```c
enum CheckEnableRlsResult
{
	RLS_NONE,
	RLS_NONE_ENV,
	RLS_ENABLED,
};
```
## Detailed Description
RLS_ENABLED serves as a return value from the  function to indicate that Row Level Security policies should be enforced for the current query context. This value is returned when:

1. The relation has Row Level Security enabled ()
2. The current user does not have BYPASSRLS privileges 
3. The user is not the table owner, or if they are the owner, FORCE ROW LEVEL SECURITY is set on the table
4. The  GUC is enabled (or  parameter is true)

When RLS_ENABLED is returned, it signals to the query planner and executor that:
- RLS policies must be applied to filter rows based on the current user's permissions
- The query plan cache should be invalidated if the security environment (user role, GUC settings) changes
- Error handling should consider that restricted data may be involved

This differs from  (no RLS on relation) and  (RLS exists but is bypassed for current environment).

## Parameters / Member Variables
As an enum value, RLS_ENABLED has no parameters or members, but it is used as a return value with the following meaning:
- Indicates active RLS enforcement is required
- Triggers plan cache invalidation on environment changes
- Signals that row-level filtering policies should be applied

## Dependencies
- Functions called/Symbols referenced:
  -  (returns this value)
  
- Called from (representative examples):
  -  (src/backend/access/index/genam.c:205)
  -  (src/backend/commands/copy.c:182) 
  -  (src/backend/commands/createas.c:536)
  -  (src/backend/executor/execMain.c:2236)
  -  (src/backend/replication/logical/tablesync.c:1528)
  -  (src/backend/utils/adt/ri_triggers.c:2526)
  -  (src/backend/utils/misc/rls.c:149)
  -  (src/backend/utils/misc/rls.c:166)

## Notes and Other Information
- [RLS_ENABLED](RLS_ENABLED.md) is part of PostgreSQL's Row Level Security feature introduced to provide fine-grained access control at the row level
- When  parameter is true in , RLS_ENABLED may be returned even when  GUC is disabled, allowing callers to test RLS applicability without triggering errors
- The return of RLS_ENABLED requires query plan cache invalidation because RLS policies are sensitive to the current user context and security settings
- This value is critical for maintaining data security in multi-tenant applications and environments where different users should see different subsets of table data
- The enum is defined in  and is widely used throughout the PostgreSQL codebase wherever RLS decisions need to be made