# pgstat_drop_function

## Location
[src/backend/utils/activity/pgstat_function.c:60-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_function.c#L60-L71)

## Overview
Removes a function from PostgreSQL's statistics tracking system when the function is dropped, ensuring proper cleanup of statistics data upon transaction commit.

## Definition
```c
void pgstat_drop_function(Oid proid)
```

## Detailed Description
This function unregisters a function from PostgreSQL's statistics tracking subsystem when the function is being dropped. It acts as a wrapper around `pgstat_drop_transactional`, specifically handling function-type statistics objects. The function ensures that statistics tracking for the specified function will be properly removed when the current transaction commits. This maintains consistency between the system catalogs and the statistics tracking system. The function relies on the coordination with `pgstat_init_function_usage` to ensure reliable operation.

## Parameters / Member Variables
- `proid`: The object identifier (OID) of the function being removed from statistics tracking

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_drop_transactional](pgstat_drop_transactional.md)
  - PGSTAT_KIND_FUNCTION
  - MyDatabaseId
- Called from (representative examples):
  - [RemoveFunctionById](../R/RemoveFunctionById.md) (in src/backend/commands/functioncmds.c:1316)

## Notes and Other Information
- This function is called during function removal to ensure proper statistics cleanup
- The statistics removal is transactional - the statistics entry will only be removed if the transaction dropping the function commits
- Depends on coordination with pgstat_init_function_usage for reliable operation
- Located in src/backend/utils/activity/pgstat_function.c:60-71
- Part of PostgreSQL's transactional statistics management ensuring consistency between catalog and statistics data