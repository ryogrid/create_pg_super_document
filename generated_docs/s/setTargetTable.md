# setTargetTable

## Location
[src/backend/parser/parse_clause.c:180-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L180-L254)

## Overview
Adds the target relation of INSERT/UPDATE/DELETE/MERGE statements to the range table and establishes special links to it in the ParseState, while acquiring necessary write locks.

## Definition

```c
int
setTargetTable(ParseState *pstate, RangeVar *relation,
			   bool inh, bool alsoSource, AclMode requiredPerms)
```
## Detailed Description
The `setTargetTable` function is essential for processing data modification statements (DML) in PostgreSQL. It performs several critical tasks:

1. **ENR conflict checking**: Ensures that Ephemeral Named Relations (ENRs) don't conflict with the target table name
2. **Lock acquisition**: Opens the target relation and acquires a write lock (RowExclusiveLock) that will be held until transaction end
3. **Range table entry creation**: Creates an RTE and ParseNamespaceItem for the target relation
4. **Permission management**: Sets the required permissions on the target relation
5. **Conditional namespace addition**: Optionally adds the target to the query's joinlist and namespace based on the statement type

The function handles different DML statement types appropriately:
- **INSERT**: Target is not added to joinlist/namespace (destination only)
- **UPDATE/DELETE**: Target is added to joinlist/namespace (needs to be scanned/joined)
- **MERGE**: Target is handled like INSERT initially (added separately later)

Lock acquisition occurs before processing the FROM list to ensure write locks are obtained before any read locks if the target appears in both contexts.

## Parameters / Member Variables
- `pstate`: The current parse state containing parsing context and target relation information
- `relation`: RangeVar specifying the target relation (table name, schema, alias)
- `inh`: Boolean indicating whether to include inheritance children in the operation
- `alsoSource`: Boolean indicating whether the target should also be treated as a source (true for UPDATE/DELETE, false for INSERT/MERGE)
- `requiredPerms`: AclMode specifying the permissions required on the target relation

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [scanNameSpaceForENR](scanNameSpaceForENR.md)
  - [parserOpenTable](../p/parserOpenTable.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addNSItemToQuery](../a/addNSItemToQuery.md)
- Called from (representative examples):
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [transformInsertStmt](../t/transformInsertStmt.md)
  - [transformUpdateStmt](../t/transformUpdateStmt.md)
  - [transformMergeStmt](../t/transformMergeStmt.md)

## Notes and Other Information
- Must be called before processing the FROM list to ensure proper lock ordering
- Handles cleanup of previous target relations for multi-action rules
- The write lock is held until transaction end and is not released by free_parsestate()
- ENRs (Ephemeral Named Relations) take precedence over regular tables with the same name
- Permission checking is customized based on the specific DML operation requirements
- Returns the range table index of the target relation for later reference