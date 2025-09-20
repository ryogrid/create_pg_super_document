# StorePreparedStatement

## Location
[src/backend/commands/prepare.c:389-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L389-L430)

## Overview
Stores a prepared statement and its associated cached plan source in the global hash table, managing the transition from temporary to permanent storage and ensuring statement name uniqueness.

## Definition

```c
void
StorePreparedStatement(const char *stmt_name,
					   CachedPlanSource *plansource,
					   bool from_sql)
```
## Detailed Description
StorePreparedStatement creates a new entry in the prepared statements hash table and stores the provided cached plan source with the specified statement name. The function initializes the hash table if it doesn't exist, validates that the statement name is unique, creates a hash table entry with metadata including the preparation timestamp, and moves the cached plan source to permanent memory storage. This function serves as the final step in the preparation process, making the prepared statement available for future execution.

## Parameters / Member Variables
- : Name identifier for the prepared statement (must be unique)
- : CachedPlanSource containing the parsed and planned statement (should be "unsaved")
- : Boolean flag indicating whether the statement was created via SQL PREPARE command or protocol-level preparation

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentStatementStartTimestamp](../G/GetCurrentStatementStartTimestamp.md) (gets current statement start time)
  - [InitQueryHashTable](../I/InitQueryHashTable.md) (initializes hash table if needed)
  - [hash_search](../h/hash_search.md) (searches/creates hash table entry)
  - [SaveCachedPlan](SaveCachedPlan.md) (moves plan source to permanent memory)
  - HASH_ENTER (hash operation flag for entry creation)
- Called from (representative examples):
  - [PrepareQuery](../P/PrepareQuery.md) (stores prepared statements from PREPARE command)
  - [exec_parse_message](../e/exec_parse_message.md) (stores prepared statements from protocol messages)

## Notes and Other Information
- Lazily initializes the prepared statements hash table on first use
- Enforces statement name uniqueness by checking for duplicate entries
- Records preparation timestamp using GetCurrentStatementStartTimestamp for consistent timing
- Distinguishes between SQL-level and protocol-level prepared statement creation with from_sql flag
- Moves the cached plan source to permanent memory only after successful hash table entry creation
- Assumes the input CachedPlanSource is in "unsaved" state to handle potential errors gracefully
- Part of PostgreSQL's prepared statement infrastructure enabling statement reuse and performance optimization
- The from_sql flag may be used for different cleanup or monitoring behaviors between SQL and protocol preparations