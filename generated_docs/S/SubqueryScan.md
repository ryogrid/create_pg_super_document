# SubqueryScan

## Location
src/include/nodes/plannodes.h: 598 - 603

## Overview
SubqueryScan is a PostgreSQL plan node that scans the output of a subquery in the range table, providing access to results from nested query execution.

## Definition
```c
typedef enum SubqueryScanStatus
{
    SUBQUERY_SCAN_UNKNOWN,
    SUBQUERY_SCAN_TRIVIAL,
    SUBQUERY_SCAN_NONTRIVIAL,
} SubqueryScanStatus;

typedef struct SubqueryScan
{
    Scan                scan;
    Plan               *subplan;
    SubqueryScanStatus  scanstatus;
} SubqueryScan;
```

## Detailed Description
SubqueryScan handles the scanning of subquery results within PostgreSQL's query execution framework. Although it doesn't scan a physical relation, it inherits from the Scan structure for code-sharing purposes. SubqueryScan is often necessary when expression evaluations need to be performed on subquery results that cannot be pushed down into the subquery without risking semantic changes.

The subplan field stores the execution plan for the subquery itself, intentionally kept separate from the standard lefttree field to prevent plan-tree-traversal routines from inadvertently traversing into different Query contexts. The scanstatus field caches information about whether the subquery scan is trivial (can potentially be optimized away) or non-trivial, which is determined during planning phase.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scan node fields, relation information, and target list
- `subplan`: Pointer to the execution plan for the subquery being scanned; stored separately to maintain Query context boundaries
- `scanstatus`: SubqueryScanStatus enumeration value indicating optimization status:
  - `SUBQUERY_SCAN_UNKNOWN`: Status not yet determined during planning
  - `SUBQUERY_SCAN_TRIVIAL`: Subquery scan may be optimizable or eliminable
  - `SUBQUERY_SCAN_NONTRIVIAL`: Subquery scan cannot be optimized away

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
  - SubqueryScanStatus (enumeration for optimization status)
- Called from (representative examples):
  - ExplainNode (for EXPLAIN output)
  - ExecInitNode (executor initialization)
  - ExecInitSubqueryScan (node-specific initialization)
  - create_subqueryscan_plan (plan creation)
  - make_subqueryscan (plan node construction)
  - trivial_subqueryscan (optimization analysis)
  - set_subqueryscan_references (reference setting)

## Notes and Other Information
- SubqueryScan nodes are created when subqueries appear in FROM clauses (table subqueries) or when CTEs (Common Table Expressions) are scanned
- The separate subplan field maintains proper Query context isolation during plan tree traversal
- Optimization opportunities exist for trivial subquery scans where the subquery might be eliminated or flattened
- Supports backward scanning depending on the capabilities of the underlying subplan
- The scanstatus field is primarily used during planning and optimization phases to cache expensive computations about subquery complexity
- Essential for implementing complex queries with derived tables, views, and common table expressions
- Handles parameter passing and context management between the outer query and subquery execution contexts