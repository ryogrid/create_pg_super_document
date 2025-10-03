# FetchPreparedStatement

## Location
[src/backend/commands/prepare.c:431-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L431-L462)

## Overview
Retrieves an existing prepared statement from the hash table by name, with optional error handling for non-existent statements.

## Definition

```c
PreparedStatement *
FetchPreparedStatement(const char *stmt_name, bool throwError)
```
## Detailed Description
FetchPreparedStatement is a utility function that looks up a prepared statement in the global prepared_queries hash table. The function provides flexible error handling: it can either throw an ERROR when a statement is not found (if throwError is true) or return NULL silently. The function is designed to be efficient by first checking if the hash table exists before attempting a lookup. Note that this function does not validate the referenced plancache entry, leaving that responsibility to the caller when needed.

## Parameters / Member Variables
- `*stmt_name`: The name of the prepared statement to retrieve
- `throwError`: Boolean flag controlling error behavior - if true, throws ERROR when statement not found; if false, returns NULL
## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (for hash table lookup)
  - ereport (for error reporting when throwError is true)
- Called from (representative examples):
  - [ExecuteQuery](../E/ExecuteQuery.md)
  - [DropPreparedStatement](../D/DropPreparedStatement.md)
  - [ExplainExecuteQuery](../E/ExplainExecuteQuery.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [FetchStatementTargetList](FetchStatementTargetList.md)

## Notes and Other Information
- The function safely handles the case where the prepared_queries hash table hasn't been initialized yet
- Does not force validation of the plancache entry, allowing callers to decide when validation is necessary
- Uses ERRCODE_UNDEFINED_PSTATEMENT error code for missing prepared statements
- Part of PostgreSQL's prepared statement management system in src/backend/commands/prepare.c

## Simplified Source

```c
// Simplified version of FetchPreparedStatement
PreparedStatement *FetchPreparedStatement(const char *stmt_name, bool throwError) {
    PreparedStatement *entry;

    // Step 1: Check if hash table exists and search for statement
    if (prepared_queries) {
        entry = hash_search(prepared_queries, stmt_name, HASH_FIND, NULL);
    } else {
        entry = NULL;  // Hash table not initialized
    }

    // Step 2: Handle missing statement based on error preference
    if (!entry && throwError) {
        // Throw error for missing prepared statement
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                 errmsg("prepared statement \"%s\" does not exist", stmt_name)));
    }

    // Step 3: Return the found entry (or NULL if not found and no error requested)
    return entry;
}
```

Key simplifications made:
- Added step-by-step comments to clarify the logical flow
- Simplified the hash_search call formatting for readability
- Consolidated the error handling logic with clearer comments
- Preserved the essential three-step process: check hash table, search, handle errors
- Maintained all core functionality while improving code clarity