# AlterCollationStmt

## Location
[src/include/nodes/parsenodes.h:2447-2451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2447-L2451)

## Overview
AlterCollationStmt represents the parsed form of an ALTER COLLATION statement, used to modify existing collation objects in PostgreSQL.

## Definition

```c
typedef struct AlterCollationStmt
{
	NodeTag		type;
	List	   *collname;
} AlterCollationStmt;
```
## Detailed Description
AlterCollationStmt is a parse tree node structure that represents ALTER COLLATION SQL commands. This structure is relatively simple compared to other ALTER statement nodes, containing only the essential information needed to identify the target collation object.

The structure is part of PostgreSQL's SQL parser output and is used by the utility command processing infrastructure to handle collation modification operations. Currently, PostgreSQL supports limited ALTER COLLATION functionality, primarily for refreshing collation versions when the underlying system collation changes.

## Parameters / Member Variables
- : NodeTag for node type identification in PostgreSQL's node system
- : List of strings representing the qualified name of the collation to be altered (e.g., schema.collation_name)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [AlterCollation](AlterCollation.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Part of PostgreSQL's parse tree node system, inheriting from the standard Node structure
- The structure is intentionally minimal as ALTER COLLATION operations are limited in scope
- The collname field uses PostgreSQL's standard qualified name representation as a List of strings
- Primary use case is for refreshing collation versions when system locale data changes
- Processed by the collation command infrastructure in src/backend/commands/collationcmds.c