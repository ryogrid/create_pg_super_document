# CteScan

## Location
src/include/nodes/plannodes.h: 640 - 645

## Overview
CteScan represents a plan node for scanning Common Table Expression (CTE) results in PostgreSQL's query execution tree, enabling access to previously computed CTE data through parameter-based references.

## Definition
```c
typedef struct CteScan
{
    Scan        scan;
    int         ctePlanId;      /* ID of init SubPlan for CTE */
    int         cteParam;       /* ID of Param representing CTE output */
} CteScan;
```

## Detailed Description
CteScan is a specialized plan node that handles the scanning of Common Table Expression (CTE) results, also known as WITH clauses in SQL. It extends the base Scan node to provide functionality for accessing data that was previously computed and stored by a CTE's initialization plan. This node type is crucial for implementing the SQL WITH clause functionality that allows defining temporary named result sets that can be referenced multiple times within a query.

The node uses two key identifiers: ctePlanId references the SubPlan that computes the CTE data, and cteParam identifies the parameter that holds the CTE's output. This design allows multiple references to the same CTE to share the computed results efficiently, avoiding redundant computation while supporting the SQL standard's semantics for CTEs.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scanning information like target lists, qualifications, and plan node metadata
- `ctePlanId`: Integer ID of the initialization SubPlan responsible for computing the CTE data
- `cteParam`: Integer ID of the parameter that represents and provides access to the CTE output data

## Dependencies
- Functions called/Symbols referenced:
  - Scan (base structure)
  
- Called from (representative examples):
  - ExecInitCteScan (executor initialization)
  - create_ctescan_plan (plan creation)
  - make_ctescan (plan node construction)
  - set_plan_refs (plan reference setting)
  - finalize_plan (plan finalization)
  - set_deparse_plan (plan deparsing for rule utilities)

## Notes and Other Information
- Essential for implementing SQL WITH clauses and Common Table Expressions
- Enables efficient sharing of computed results across multiple CTE references
- Part of PostgreSQL's support for advanced SQL features and query optimization
- Works in conjunction with SubPlan nodes to manage CTE computation and storage
- Supports both non-recursive and recursive CTE scenarios
- Critical for complex analytical queries that benefit from intermediate result caching
- Integrates with PostgreSQL's parameter mechanism for efficient data passing
- Used extensively in reporting and analytical workloads that require temporary named result sets