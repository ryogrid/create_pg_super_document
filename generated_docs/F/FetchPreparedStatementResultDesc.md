# FetchPreparedStatementResultDesc

## Location
[src/backend/commands/prepare.c:463-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L463-L485)

## Overview
Retrieves the result tuple descriptor for a prepared statement, returning a copy that describes the structure of tuples the statement will produce when executed.

## Definition

```c
TupleDesc
FetchPreparedStatementResultDesc(PreparedStatement *stmt)
```
## Detailed Description
FetchPreparedStatementResultDesc extracts and returns the result tuple descriptor from a prepared statement's plan source. The function is optimized based on the assumption that prepared statements have fixed result tuple descriptors that do not change, eliminating the need for plan revalidation. If the prepared statement will return tuples, the function creates a copy of the result descriptor in the current memory context. If the statement does not return tuples (such as INSERT, UPDATE, DELETE without RETURNING), it returns NULL.

## Parameters / Member Variables
- `*stmt`: Pointer to the PreparedStatement from which to extract the result tuple descriptor
## Dependencies
- Functions called/Symbols referenced:
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md) (creates a copy of the tuple descriptor)
  - Assert (for debugging validation)
- Called from (representative examples):
  - [UtilityTupleDescriptor](../U/UtilityTupleDescriptor.md)

## Notes and Other Information
- Assumes prepared statements have fixed result descriptors (stmt->plansource->fixed_result must be true)
- Returns a copy in the current memory context, not the original descriptor
- No plan revalidation is performed since result descriptors for prepared statements are immutable
- Returns NULL for statements that do not produce result tuples
- Part of PostgreSQL's prepared statement result introspection system

## Simplified Source

```c
// Simplified version of FetchPreparedStatementResultDesc
TupleDesc
FetchPreparedStatementResultDesc(PreparedStatement *stmt)
{
    // Assert that prepared statement has fixed result descriptor
    Assert(stmt->plansource->fixed_result);

    // If statement produces result tuples, return a copy of the descriptor
    if (stmt->plansource->resultDesc)
        return CreateTupleDescCopy(stmt->plansource->resultDesc);

    // Otherwise, statement doesn't return tuples (e.g., INSERT/UPDATE/DELETE)
    return NULL;
}
```

Key simplifications made:
- Preserved the core assertion check for fixed result descriptors
- Maintained the essential logic flow: check for result descriptor existence
- Kept the memory management aspect (CreateTupleDescCopy for current context)
- Added clear comments explaining each logical step
- Removed detailed comment block but preserved essential functionality