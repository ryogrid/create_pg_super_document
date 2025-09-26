# SetOperationStmt

## Location
src/include/nodes/parsenodes.h: 2185 - 2204

## Overview
SetOperationStmt represents a set operation node (UNION, INTERSECT, EXCEPT) in post-analysis query trees, forming a binary tree structure with type information and execution details for combining query results.

## Definition
```c
typedef struct SetOperationStmt
{
    NodeTag      type;
    SetOperation op;            /* type of set op */
    bool         all;           /* ALL specified? */
    Node        *larg;          /* left child */
    Node        *rarg;          /* right child */
    
    /* Fields derived during parse analysis (irrelevant for query jumbling): */
    List        *colTypes;      /* OID list of output column type OIDs */
    List        *colTypmods;    /* integer list of output column typmods */
    List        *colCollations; /* OID list of output column collation OIDs */
    List        *groupClauses;  /* a list of SortGroupClause's */
    /* groupClauses is NIL if UNION ALL, but must be set otherwise */
} SetOperationStmt;
```

## Detailed Description
SetOperationStmt is a specialized node used in post-analysis query trees to represent set operations after SELECT statements have been parsed and analyzed. Unlike SelectStmt which represents the raw parsed form, SetOperationStmt contains resolved type information and execution details. It forms a binary tree where leaf nodes are typically RangeTblRef nodes (representing the actual SELECT queries) and internal nodes are SetOperationStmt nodes representing the set operations. The structure includes detailed type information about output columns and grouping clauses needed for duplicate elimination in non-UNION-ALL operations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a SetOperationStmt node type
- `op`: SetOperation enum specifying the type of set operation (UNION, INTERSECT, EXCEPT)
- `all`: Boolean flag indicating whether ALL was specified (affects duplicate handling)
- `larg`: Left child node (typically another SetOperationStmt or RangeTblRef)
- `rarg`: Right child node (typically another SetOperationStmt or RangeTblRef)
- `colTypes`: List of OIDs representing the data types of output columns
- `colTypmods`: List of integers representing type modifiers for output columns
- `colCollations`: List of OIDs representing collations for output columns
- `groupClauses`: List of SortGroupClause nodes for duplicate elimination (NULL for UNION ALL)

## Dependencies
- Functions called/Symbols referenced:
  - SetOperation
- Called from (representative examples):
  - plan_set_operations
  - transformSetOperationStmt
  - recurse_set_operations
  - generate_union_paths
  - generate_nonunion_paths
  - subquery_planner

## Notes and Other Information
- SetOperationStmt is used in the post-analysis phase, replacing the raw SelectStmt tree structure
- Contains resolved type information that enables proper type coercion and validation
- groupClauses is NULL for UNION ALL operations since they don't require duplicate elimination
- The binary tree structure allows for complex nested set operations
- Type information includes collations which are crucial for string comparison and sorting
- Used extensively in query planning and optimization phases for set operation execution
- The pg_node_attr(query_jumble_ignore) annotations indicate fields not relevant for query fingerprinting
- Supports the full SQL standard for set operations including proper type resolution and duplicate handling
- Critical for implementing efficient set operation execution strategies in the query planner