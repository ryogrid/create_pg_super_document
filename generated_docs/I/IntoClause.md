# IntoClause

## Location
[src/include/nodes/primnodes.h:158-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L158-L171)

## Overview
IntoClause represents target information for SELECT INTO, CREATE TABLE AS, and CREATE MATERIALIZED VIEW operations, providing comprehensive table creation specifications.

## Definition

```c
typedef struct IntoClause
{
	NodeTag		type;

	RangeVar   *rel;			/* target relation name */
	List	   *colNames;		/* column names to assign, or NIL */
	char	   *accessMethod;	/* table access method */
	List	   *options;		/* options from WITH clause */
	OnCommitAction onCommit;	/* what do we do at COMMIT? */
	char	   *tableSpaceName; /* table space to use, or NULL */
	/* materialized view's SELECT query */
	Node	   *viewQuery pg_node_attr(query_jumble_ignore);
	bool		skipData;		/* true for WITH NO DATA */
} IntoClause;
```
## Detailed Description
IntoClause is a specialized node structure that encapsulates all the information needed to create a new table or materialized view from a SELECT statement. It serves as the bridge between query execution and table creation, supporting various SQL constructs including SELECT INTO, CREATE TABLE AS, and CREATE MATERIALIZED VIEW.

The structure provides comprehensive control over table creation aspects including target relation specification, column naming, storage access methods, table options, transaction behavior, and tablespace placement. For materialized views, it includes the parsed SELECT query and data population control.

The viewQuery field is specifically used for CREATE MATERIALIZED VIEW to store the parsed but not rewritten SELECT query, and is excluded from query jumbling since CreateTableAsStmt already references its own Query.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL's node system type identification
- : RangeVar pointer specifying the target relation name and optional schema
- : List of String nodes specifying custom column names, or NIL to use query column names
- : String specifying the table access method (e.g., 'heap', custom access methods)
- : List of DefElem nodes from WITH clause specifying table creation options
- : OnCommitAction enum controlling transaction commit behavior (PRESERVE_ROWS, DELETE_ROWS, DROP)
- : String specifying tablespace for table placement, or NULL for default
- : Node pointer to materialized view's SELECT query (excluded from query jumbling)
- : Boolean flag indicating WITH NO DATA clause (true = don't populate data)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node system)
  - [RangeVar](../R/RangeVar.md) (target relation specification)
  - [List](../L/List.md) (PostgreSQL list structure)
  - OnCommitAction (transaction behavior enum)
  - [Node](../N/Node.md) (generic node structure)

- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md) (execution of CREATE TABLE AS)
  - [create_ctas_internal](../c/create_ctas_internal.md) (internal table creation)
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (result destination setup)
  - [ExplainOneQuery](../E/ExplainOneQuery.md) (EXPLAIN processing)
  - [intorel_startup](../i/intorel_startup.md)/shutdown (relation lifecycle management)
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md) (query explanation)

## Notes and Other Information
- Central to PostgreSQL's table creation from query results functionality
- Supports multiple SQL constructs: SELECT INTO, CREATE TABLE AS, CREATE MATERIALIZED VIEW
- Provides fine-grained control over table creation parameters
- Transaction behavior control through onCommit for temporary table handling
- Access method specification enables custom storage engines
- Column name override capability for result customization
- Materialized view query storage for view definition persistence
- WITH NO DATA support for schema-only table creation
- Tablespace specification for storage placement control
- Critical component in the query-to-table transformation pipeline