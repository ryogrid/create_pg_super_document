# FetchPreparedStatement

## Location
[src/backend/commands/prepare.c:431-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L431-L462)

## Overview
Retrieves an existing prepared statement from the hash table by name, with optional error handling for non-existent statements.

## Definition


## Detailed Description
FetchPreparedStatement is a utility function that looks up a prepared statement in the global prepared_queries hash table. The function provides flexible error handling: it can either throw an ERROR when a statement is not found (if throwError is true) or return NULL silently. The function is designed to be efficient by first checking if the hash table exists before attempting a lookup. Note that this function does not validate the referenced plancache entry, leaving that responsibility to the caller when needed.

## Parameters / Member Variables
- : The name of the prepared statement to retrieve
- : Boolean flag controlling error behavior - if true, throws ERROR when statement not found; if false, returns NULL

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