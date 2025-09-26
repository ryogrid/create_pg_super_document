# CreateTableAsStmt

## Location
[src/include/nodes/parsenodes.h:3888-3896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3888-L3896)

## Overview
CreateTableAsStmt represents the parsed structure of CREATE TABLE AS and SELECT INTO statements, as well as CREATE MATERIALIZED VIEW statements, which all share similar functionality for creating tables from query results.

## Definition

```c
typedef struct CreateTableAsStmt
{
	NodeTag		type;
	Node	   *query;			/* the query (see comments above) */
	IntoClause *into;			/* destination table */
	ObjectType	objtype;		/* OBJECT_TABLE or OBJECT_MATVIEW */
	bool		is_select_into; /* it was written as SELECT INTO */
	bool		if_not_exists;	/* just do nothing if it already exists? */
} CreateTableAsStmt;
```
## Detailed Description
CreateTableAsStmt unifies the representation of several related SQL constructs that create tables from query results. It handles CREATE TABLE AS statements natively, transforms SELECT ... INTO statements during parse analysis, and also represents CREATE MATERIALIZED VIEW statements since they require the same underlying data structures. The query field can contain either a SELECT or EXECUTE statement, but not other DML statements. This design provides a common framework for all table-creation-from-query operations.

## Parameters / Member Variables
- : NodeTag identifying this as a CreateTableAsStmt node
- : Pointer to the query node (SELECT or EXECUTE) that provides the data
- : IntoClause specifying the destination table details and options
- : ObjectType indicating whether this creates a regular table (OBJECT_TABLE) or materialized view (OBJECT_MATVIEW)
- : Boolean flag indicating if the original syntax was SELECT INTO (vs CREATE TABLE AS)
- : Boolean flag for IF NOT EXISTS clause - when true, no error if table already exists

## Dependencies
- Functions called/Symbols referenced:
  - [IntoClause](../I/IntoClause.md) (for destination table specification)
  - ObjectType (for object type classification)
- Called from (representative examples):
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [CreateTableAsRelExists](CreateTableAsRelExists.md)
  - [ExplainOneUtility](../E/ExplainOneUtility.md)
  - [DefineView](../D/DefineView.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [transformOptionalSelectInto](../t/transformOptionalSelectInto.md)
  - [transformCreateTableAsStmt](../t/transformCreateTableAsStmt.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)
  - [UtilityContainsQuery](../U/UtilityContainsQuery.md)
  - [CreateCommandTag](CreateCommandTag.md)

## Notes and Other Information
- Provides unified handling for CREATE TABLE AS, SELECT INTO, and CREATE MATERIALIZED VIEW
- The transformation from SELECT INTO to CREATE TABLE AS form happens during parse analysis
- [Query](../Q/Query.md) field is restricted to SELECT and EXECUTE statements for data safety
- The is_select_into flag preserves the original syntax information for proper error reporting and logging
- Integration with EXPLAIN allows users to see execution plans for table creation operations
- Used extensively in stored procedure language implementations through SPI