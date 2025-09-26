# UpdateStmt

## Location
[src/include/nodes/parsenodes.h:2069-2078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2069-L2078)

## Overview
UpdateStmt represents the parsed structure of an UPDATE statement in PostgreSQL, containing all necessary information to modify existing rows in a table with optional conditions, joins, and return values.

## Definition
```c
typedef struct UpdateStmt
{
    NodeTag     type;
    RangeVar   *relation;        /* relation to update */
    List       *targetList;      /* the target list (of ResTarget) */
    Node       *whereClause;     /* qualifications */
    List       *fromClause;      /* optional from clause for more tables */
    List       *returningList;   /* list of expressions to return */
    WithClause *withClause;      /* WITH clause */
} UpdateStmt;
```

## Detailed Description
UpdateStmt is a parse tree node that represents an UPDATE statement after SQL parsing. It encapsulates all components of an UPDATE operation including the target table, the list of columns and their new values, optional WHERE conditions for filtering rows to update, FROM clause for joining additional tables, RETURNING clause for retrieving updated values, and WITH clause for common table expressions. This structure is created during the parsing phase and later transformed into execution plans by the query planner.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an UpdateStmt node type
- `relation`: RangeVar pointer specifying the target table to update
- `targetList`: List of ResTarget nodes specifying which columns to update and their new values
- `whereClause`: Node containing the WHERE condition to determine which rows to update
- `fromClause`: Optional list of additional tables/relations for complex update operations with joins
- `returningList`: List of expressions specifying what values to return from updated rows
- `withClause`: WithClause pointer for common table expressions (CTEs) used in the statement

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar
  - WithClause
- Called from (representative examples):
  - transformStmt
  - transformUpdateStmt
  - raw_expression_tree_walker_impl
  - transformWithClause
  - makeDependencyGraphWalker

## Notes and Other Information
- UpdateStmt is part of the parse tree node hierarchy and inherits from the base Node structure
- The targetList contains ResTarget nodes that specify column assignments (column = new_value)
- FROM clause enables complex updates involving joins with other tables
- RETURNING clause allows retrieving values from updated rows, useful for triggers and application logic
- WITH clause support enables use of CTEs in UPDATE statements for complex data manipulation scenarios
- This node is transformed during query analysis phase into execution-ready structures
- The structure supports both simple single-table updates and complex multi-table update operations