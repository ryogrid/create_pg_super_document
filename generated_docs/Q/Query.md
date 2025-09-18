# Query

## Location
src/include/nodes/parsenodes.h: 117 - 241

## Overview
The Query structure is the central parse tree representation in PostgreSQL that all SQL statements are converted into during parse analysis for further processing by the rewriter and planner.

## Definition
```c
typedef struct Query
{
    NodeTag     type;
    CmdType     commandType;    /* select|insert|update|delete|merge|utility */
    QuerySource querySource;
    uint64      queryId;
    bool        canSetTag;
    Node       *utilityStmt;    /* non-null if commandType == CMD_UTILITY */
    int         resultRelation; /* rtable index of target relation */
    // ... (additional fields for query analysis and execution)
} Query;
```

## Detailed Description
The Query structure serves as the universal parse tree format that PostgreSQL uses to represent all SQL statements after initial parsing. Parse analysis converts raw SQL statements into this standardized Query tree format, which is then processed by the rewriter for rule application and view expansion, and subsequently by the planner to generate execution plans.

For utility statements (non-optimizable statements like CREATE, DROP, etc.), the utilityStmt field is populated and most other Query fields remain unused. For optimizable statements (SELECT, INSERT, UPDATE, DELETE, MERGE), the Query structure contains comprehensive information about tables, joins, target lists, qualifications, grouping, sorting, and other query semantics.

The Query structure is extensively used throughout query processing but is not directly used by the executor, which works with PlannedStmt nodes instead.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a Query node
- `commandType`: Type of SQL command (SELECT, INSERT, UPDATE, DELETE, MERGE, or UTILITY)
- `querySource`: Origin context of the query (ignored for query jumbling)
- `queryId`: Unique identifier for the query based on jumbling (plugin-settable)
- `canSetTag`: Whether this query sets the command result tag
- `utilityStmt`: Pointer to utility statement node (non-null for CMD_UTILITY)
- `resultRelation`: Range table index of target relation for DML operations (0 for SELECT)
- `hasAggs`: Boolean flag indicating presence of aggregates in target list or HAVING clause
- `hasWindowFuncs`: Boolean flag for window functions in target list
- `hasTargetSRFs`: Boolean flag for set-returning functions in target list
- `hasSubLinks`: Boolean flag for subquery SubLinks
- `hasDistinctOn`: Boolean flag indicating DISTINCT ON usage
- `hasRecursive`: Boolean flag for WITH RECURSIVE
- `hasModifyingCTE`: Boolean flag for INSERT/UPDATE/DELETE/MERGE in WITH clause
- `hasForUpdate`: Boolean flag for FOR [KEY] UPDATE/SHARE clauses
- `hasRowSecurity`: Boolean flag indicating RLS policy application
- `isReturn`: Boolean flag for RETURN statements
- `cteList`: List of CommonTableExpr nodes (WITH clause)
- `rtable`: List of range table entries
- `rteperminfos`: List of RTEPermissionInfo nodes for permission tracking
- `jointree`: FromExpr representing table join tree (FROM and WHERE clauses)
- `mergeActionList`: List of actions for MERGE statements
- `mergeTargetRelation`: Range table index for MERGE target relation
- `mergeJoinCondition`: Join condition between source and target for MERGE
- `targetList`: List of TargetEntry nodes representing the SELECT list
- `override`: OVERRIDING clause specification
- `onConflict`: ON CONFLICT expression for INSERT statements
- `returningList`: RETURNING clause target list
- `groupClause`: List of SortGroupClause nodes for GROUP BY
- `groupDistinct`: Boolean indicating distinct grouping
- `groupingSets`: List of GroupingSet nodes for advanced grouping
- `havingQual`: HAVING clause qualifications
- `windowClause`: List of WindowClause nodes
- `distinctClause`: List of SortGroupClause nodes for DISTINCT
- `sortClause`: List of SortGroupClause nodes for ORDER BY
- `limitOffset`: Expression for OFFSET clause
- `limitCount`: Expression for LIMIT clause
- `limitOption`: LIMIT clause options (ALL, etc.)
- `rowMarks`: List of RowMarkClause nodes for locking clauses
- `setOperations`: SetOperationStmt tree for UNION/INTERSECT/EXCEPT
- `constraintDeps`: List of constraint OIDs for semantic validity
- `withCheckOptions`: List of WithCheckOption nodes (added during rewrite)
- `stmt_location`: Source text location of query start
- `stmt_len`: Length of query in source text

## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - QuerySource
  - FromExpr
  - OverridingKind
  - OnConflictExpr
  - LimitOption
  - ParseLoc
- Called from (representative examples):
  - No direct references found (base structure used throughout system)

## Notes and Other Information
- The Query structure is the cornerstone of PostgreSQL's query processing pipeline
- Many fields are marked with pg_node_attr annotations for query jumbling, equality comparison, and serialization control
- Query jumbling is used to generate query fingerprints for statistics and plan caching
- The structure supports all major SQL operations and advanced features like CTEs, window functions, and MERGE statements
- Fields marked with query_jumble_ignore do not contribute to the query identifier calculation
- The Query tree is transformed through multiple processing phases but maintains its central role until plan generation