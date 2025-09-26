# MergeStmt

## Location
[src/include/nodes/parsenodes.h:2084-2093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2084-L2093)

## Overview
MergeStmt represents the parsed structure of a MERGE statement in PostgreSQL, containing all necessary information to perform conditional INSERT, UPDATE, or DELETE operations based on whether source rows match target table rows.

## Definition
```c
typedef struct MergeStmt
{
    NodeTag     type;
    RangeVar   *relation;          /* target relation to merge into */
    Node       *sourceRelation;    /* source relation */
    Node       *joinCondition;     /* join condition between source and target */
    List       *mergeWhenClauses;  /* list of MergeWhenClause(es) */
    List       *returningList;     /* list of expressions to return */
    WithClause *withClause;        /* WITH clause */
} MergeStmt;
```

## Detailed Description
MergeStmt is a parse tree node that represents a MERGE statement after SQL parsing. It encapsulates all components of a MERGE operation including the target table, source data (table, subquery, or values), join condition that determines how source and target rows are matched, a list of WHEN clauses that specify actions to take based on match conditions, optional RETURNING clause for retrieving affected values, and WITH clause for common table expressions. This structure enables the SQL standard MERGE functionality for conditional multi-action data manipulation.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a MergeStmt node type
- `relation`: RangeVar pointer specifying the target table to merge into
- `sourceRelation`: Node containing the source data (table, subquery, or values clause)
- `joinCondition`: Node specifying the join condition that determines row matching between source and target
- `mergeWhenClauses`: List of MergeWhenClause nodes defining conditional actions (WHEN MATCHED, WHEN NOT MATCHED)
- `returningList`: List of expressions specifying what values to return from affected rows
- `withClause`: WithClause pointer for common table expressions (CTEs) used in the statement

## Dependencies
- Functions called/Symbols referenced:
  - RangeVar
  - WithClause
- Called from (representative examples):
  - transformStmt
  - transformMergeStmt
  - raw_expression_tree_walker_impl
  - transformWithClause
  - makeDependencyGraphWalker

## Notes and Other Information
- MergeStmt is part of the parse tree node hierarchy and inherits from the base Node structure
- MERGE statements provide SQL standard functionality for conditional INSERT/UPDATE/DELETE operations
- The mergeWhenClauses list contains conditions and actions for different match scenarios
- JOIN condition determines how source rows are matched against target table rows
- RETURNING clause allows retrieving values from all affected rows regardless of the action taken
- WITH clause support enables use of CTEs in MERGE statements for complex data sources
- This node is transformed during query analysis phase into execution-ready structures
- MERGE statements are particularly useful for ETL operations and data synchronization scenarios