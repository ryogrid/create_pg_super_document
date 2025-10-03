# DropPreparedStatement

## Location
[src/backend/commands/prepare.c:516-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L516-L536)

## Overview
Internal implementation function that removes a specific prepared statement from the hash table and releases its associated cached plan resources.

## Definition

```c
void
DropPreparedStatement(const char *stmt_name, bool showError)
```
## Detailed Description
DropPreparedStatement is the core implementation function for removing individual prepared statements from PostgreSQL's prepared statement system. It performs a two-step cleanup process: first releasing the cached plan resources through the plancache system, then removing the hash table entry that tracks the prepared statement. The function provides flexible error handling, allowing callers to choose whether missing statements should generate errors or be silently ignored. This internal function is used by both the DEALLOCATE command and system cleanup routines.

## Parameters / Member Variables
- `*stmt_name`: The name of the prepared statement to remove from the system
- `showError`: Boolean flag controlling error behavior - if true, missing statements cause errors; if false, missing statements are silently ignored
## Dependencies
- Functions called/Symbols referenced:
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (locates the statement in the hash table)
  - [DropCachedPlan](DropCachedPlan.md) (releases plancache resources)
  - [hash_search](../h/hash_search.md) (removes entry from hash table with HASH_REMOVE)
- Called from (representative examples):
  - [DeallocateQuery](DeallocateQuery.md) (user-initiated DEALLOCATE commands)
  - [PostgresMain](../P/PostgresMain.md) (session cleanup)

## Notes and Other Information
- Performs proper resource cleanup by releasing cached plans before removing hash table entries
- Safe to call on non-existent statements when showError is false (no-op behavior)
- Part of the internal prepared statement management system, not directly exposed to SQL
- Ensures proper cleanup order: plancache first, then hash table entry
- Used during both explicit deallocation and automatic session cleanup
- The function handles the case where FetchPreparedStatement returns NULL gracefully

## Simplified Source

```c
// Simplified version of DropPreparedStatement
void DropPreparedStatement(const char *stmt_name, bool showError) {
    PreparedStatement *entry;

    // Step 1: Look up the prepared statement in the hash table
    // If showError is true, this will raise an error for missing statements
    entry = FetchPreparedStatement(stmt_name, showError);

    // Step 2: Clean up resources if statement was found
    if (entry) {
        // First, release the cached execution plan
        DropCachedPlan(entry->plansource);

        // Then, remove the statement from the hash table
        hash_search(prepared_queries, entry->stmt_name, HASH_REMOVE, NULL);
    }
}
```

Key simplifications made:
- Added clear step-by-step comments explaining the two-phase cleanup process
- Emphasized the conditional logic for handling missing statements
- Focused on the main execution path with clearer variable naming context
- Highlighted the proper cleanup order (cached plan first, then hash table entry)