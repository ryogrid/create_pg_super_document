# ClusterStmt

## Location
src/include/nodes/parsenodes.h: 3822 - 3828

## Overview
A parse node structure representing the CLUSTER statement, used to reorganize tables by physically reordering rows according to an index.

## Definition

```c
typedef struct ClusterStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation being indexed, or NULL if all */
	char	   *indexname;		/* original index defined */
	List	   *params;			/* list of DefElem nodes */
} ClusterStmt;
```
## Detailed Description
ClusterStmt is a parse node structure that represents a CLUSTER SQL statement. This structure is created during parsing of SQL commands like "CLUSTER table_name USING index_name", "CLUSTER table_name" (uses previously clustered index), or "CLUSTER" (clusters all tables that have a previously clustered index). The CLUSTER command physically reorders the rows of a table according to the sort order of an index, which can improve query performance for queries that follow the same ordering.

The clustering operation requires an exclusive lock on the table and can be time-consuming for large tables. It supports both single-table clustering and clustering multiple tables in separate transactions to avoid deadlocks.

## Parameters / Member Variables
- : NodeTag identifying this as a ClusterStmt node
- : Pointer to RangeVar identifying the target table, or NULL to cluster all tables with clustered indexes
- : String containing the name of the index to cluster on, or NULL to use the previously clustered index
- : List of DefElem nodes containing options (currently supports VERBOSE option)

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar (for table identification)
  - List (PostgreSQL list structure for parameters)
  - DefElem (for option definitions)
- Called from (representative examples):
  - cluster (execution function in cluster.c)
  - standard_ProcessUtility (utility command processing)

## Notes and Other Information
- This structure is part of the PostgreSQL parser node hierarchy and inherits from Node via the NodeTag
- If no index is specified, PostgreSQL looks for an index with the indisclustered bit set
- CLUSTER without a table name processes all tables that have been previously clustered
- The operation is not transactional for multiple tables - each table is processed in its own transaction
- Requires exclusive table locks, which can cause blocking
- Partitioned tables are supported by clustering each partition individually
- The VERBOSE option provides progress information during clustering
- Remote temporary tables cannot be clustered due to buffer manager limitations