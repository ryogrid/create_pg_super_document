# DeleteStmt

## Location
[src/include/nodes/parsenodes.h:2055-2063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2055-L2063)

## Overview
DeleteStmt represents the parsed structure of a DELETE statement in PostgreSQL, containing all necessary information to delete rows from a table with optional conditions, joins, and return values.

## Definition

```c
typedef struct DeleteStmt
{
	NodeTag		type;
	RangeVar   *relation;		/* relation to delete from */
	List	   *usingClause;	/* optional using clause for more tables */
	Node	   *whereClause;	/* qualifications */
	List	   *returningList;	/* list of expressions to return */
	WithClause *withClause;		/* WITH clause */
} DeleteStmt;
```
## Detailed Description
DeleteStmt is a parse tree node that represents a DELETE statement after SQL parsing. It encapsulates all components of a DELETE operation including the target table, optional USING clause for multi-table deletes, WHERE conditions for filtering rows to delete, RETURNING clause for retrieving deleted values, and WITH clause for common table expressions. This structure is created during the parsing phase and later transformed into execution plans by the query planner.

## Parameters / Member Variables
- : NodeTag identifying this as a DeleteStmt node type
- : RangeVar pointer specifying the target table to delete from
- : Optional list of additional tables/relations for complex delete operations
- : Node containing the WHERE condition to determine which rows to delete
- : List of expressions specifying what values to return from deleted rows
- : WithClause pointer for common table expressions (CTEs) used in the statement

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](../R/RangeVar.md)
  - [WithClause](../W/WithClause.md)
- Called from (representative examples):
  - [transformStmt](../t/transformStmt.md)
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)
  - [transformWithClause](../t/transformWithClause.md)
  - [makeDependencyGraphWalker](../m/makeDependencyGraphWalker.md)

## Notes and Other Information
- [DeleteStmt](DeleteStmt.md) is part of the parse tree node hierarchy and inherits from the base Node structure
- The structure supports complex DELETE operations including multi-table deletes via USING clause
- RETURNING clause allows retrieving values from deleted rows, useful for triggers and application logic
- WITH clause support enables use of CTEs in DELETE statements for complex data manipulation scenarios
- This node is transformed during query analysis phase into execution-ready structures