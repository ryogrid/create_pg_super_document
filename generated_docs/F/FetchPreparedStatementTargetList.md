# FetchPreparedStatementTargetList

## Location
[src/backend/commands/prepare.c:486-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L486-L501)

## Overview
Extracts the query target list from a prepared statement that returns tuples, providing access to the column structure and expressions.

## Definition

```c
List *
FetchPreparedStatementTargetList(PreparedStatement *stmt)
```
## Detailed Description
FetchPreparedStatementTargetList retrieves the target list from a prepared statement's cached plan. The target list describes the columns and expressions that will be returned by the query. This function is primarily used in corner cases like the DESCRIBE statement on an EXECUTE command. The implementation is intentionally simple rather than optimized for performance, as it's not used in performance-critical paths. The function safely copies the target list into the caller's memory context to protect against plan invalidation.

## Parameters / Member Variables
- : Pointer to the PreparedStatement from which to extract the target list

## Dependencies
- Functions called/Symbols referenced:
  - [CachedPlanGetTargetList](../C/CachedPlanGetTargetList.md) (retrieves target list from cached plan)
  - copyObject (creates a copy in caller's context)
- Called from (representative examples):
  - [FetchStatementTargetList](FetchStatementTargetList.md)

## Notes and Other Information
- Designed for corner cases and debugging rather than performance-critical operations
- Returns NIL if the statement doesn't have a determinable target list
- Creates a copy in the caller's memory context to prevent issues with plan invalidation
- Only meaningful for prepared statements that return tuples
- The implementation prioritizes correctness and safety over performance efficiency
- Part of PostgreSQL's prepared statement introspection system for query analysis

## Simplified Source

```c
// Simplified version of FetchPreparedStatementTargetList
List *
FetchPreparedStatementTargetList(PreparedStatement *stmt)
{
    // Get the target list from the prepared statement's cached plan
    List *target_list = CachedPlanGetTargetList(stmt->plansource, NULL);

    // Return a copy in caller's context to prevent plan invalidation issues
    return copyObject(target_list);
}
```

Key simplifications made:
- Simplified variable names for clarity (tlist → target_list)
- Added descriptive comments explaining the two main steps
- Preserved the essential logic: get target list and copy it safely
- Maintained the simple structure as the original function is already quite concise