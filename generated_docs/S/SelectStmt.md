# SelectStmt

## Location
src/include/nodes/parsenodes.h: 2116 - 2163

## Overview
SelectStmt represents the parsed structure of a SELECT statement in PostgreSQL, containing all necessary information for querying data including projections, joins, filtering, grouping, ordering, and set operations.

## Definition
```c
typedef struct SelectStmt
{
    NodeTag     type;
    
    /* These fields are used only in "leaf" SelectStmts. */
    List       *distinctClause;  /* NULL, list of DISTINCT ON exprs, or
                                  * lcons(NIL,NIL) for all (SELECT DISTINCT) */
    IntoClause *intoClause;      /* target for SELECT INTO */
    List       *targetList;      /* the target list (of ResTarget) */
    List       *fromClause;      /* the FROM clause */
    Node       *whereClause;     /* WHERE qualification */
    List       *groupClause;     /* GROUP BY clauses */
    bool        groupDistinct;   /* Is this GROUP BY DISTINCT? */
    Node       *havingClause;    /* HAVING conditional-expression */
    List       *windowClause;    /* WINDOW window_name AS (...), ... */
    
    /* In a "leaf" node representing a VALUES list */
    List       *valuesLists;     /* untransformed list of expression lists */
    
    /* These fields are used in both "leaf" SelectStmts and upper-level SelectStmts. */
    List       *sortClause;      /* sort clause (a list of SortBy's) */
    Node       *limitOffset;     /* # of result tuples to skip */
    Node       *limitCount;      /* # of result tuples to return */
    LimitOption limitOption;     /* limit type */
    List       *lockingClause;   /* FOR UPDATE (list of LockingClause's) */
    WithClause *withClause;      /* WITH clause */
    
    /* These fields are used only in upper-level SelectStmts. */
    SetOperation op;             /* type of set op */
    bool        all;             /* ALL specified? */
    struct SelectStmt *larg;     /* left child */
    struct SelectStmt *rarg;     /* right child */
} SelectStmt;
```

## Detailed Description
SelectStmt is the most complex parse tree node representing SELECT statements and set operations. It serves dual purposes: as a leaf node for basic SELECT queries and as an internal node for set operations (UNION, INTERSECT, EXCEPT). For leaf nodes, it contains all standard SQL SELECT clauses including DISTINCT, INTO, target list, FROM, WHERE, GROUP BY, HAVING, WINDOW, and ordering. For set operations, it forms a binary tree structure with left and right child SelectStmt nodes. The structure also supports VALUES clauses as a special case of SELECT.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a SelectStmt node type
- `distinctClause`: List for DISTINCT ON expressions, or special marker for SELECT DISTINCT
- `intoClause`: IntoClause for SELECT INTO operations (creating tables from query results)
- `targetList`: List of ResTarget nodes specifying what columns/expressions to select
- `fromClause`: List of tables, joins, and other FROM clause elements
- `whereClause`: Node containing WHERE condition for row filtering
- `groupClause`: List of GROUP BY expressions for aggregation
- `groupDistinct`: Boolean flag for GROUP BY DISTINCT functionality
- `havingClause`: Node containing HAVING condition for group filtering
- `windowClause`: List of named window specifications
- `valuesLists`: List of value lists for VALUES clauses (alternative to SELECT)
- `sortClause`: List of SortBy nodes for ORDER BY specification
- `limitOffset`: Expression for OFFSET clause (number of rows to skip)
- `limitCount`: Expression for LIMIT clause (maximum rows to return)
- `limitOption`: LimitOption enum specifying limit behavior
- `lockingClause`: List of LockingClause nodes for row-level locking (FOR UPDATE, etc.)
- `withClause`: WithClause for common table expressions (CTEs)
- `op`: SetOperation enum for set operation type (UNION, INTERSECT, EXCEPT)
- `all`: Boolean flag indicating whether ALL was specified in set operations
- `larg`: Left child SelectStmt for set operations
- `rarg`: Right child SelectStmt for set operations

## Dependencies
- Functions called/Symbols referenced:
  - IntoClause
  - LimitOption
  - WithClause
  - SetOperation
  - SelectStmt (self-reference for tree structure)
- Called from (representative examples):
  - transformSelectStmt
  - transformStmt
  - transformSetOperationStmt
  - transformValuesClause
  - transformWithClause
  - DoCopy

## Notes and Other Information
- SelectStmt is the most versatile parse tree node, handling both simple queries and complex set operations
- The structure forms a binary tree for set operations, enabling nested UNION/INTERSECT/EXCEPT operations
- VALUES clauses are represented as special SelectStmt nodes with valuesLists populated
- Window functions are supported through the windowClause for advanced analytical queries
- Row-level locking is integrated through lockingClause for transaction control
- The distinction between leaf and internal nodes enables recursive processing of complex queries
- This node is transformed during query analysis into Query structures for optimization and execution
- Supports the full range of SQL SELECT functionality including subqueries, CTEs, and set operations