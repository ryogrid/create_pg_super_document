# add_function_cost

## Location
src/backend/optimizer/util/plancat.c: 2089 - 2149

## Overview
Estimates the execution cost of a function and adds it to an existing cost structure, supporting both one-time and per-tuple cost components.

## Definition
```c
void add_function_cost(PlannerInfo *root, Oid funcid, Node *node, QualCost *cost)
```

## Detailed Description
The add_function_cost function is a key component of PostgreSQL's query cost estimation system. It calculates the expected execution cost of calling a specific function and accumulates this cost into the provided QualCost structure. The function supports two different cost estimation mechanisms:

1. **Support Function Method**: If the function has a registered support function (prosupport), it creates a SupportRequestCost structure and calls the support function to get a custom cost estimate. This allows function authors to provide specialized cost estimation logic that takes into account the function's specific behavior and implementation details.

2. **Default Method**: If no support function exists or it fails, the function falls back to using the procost value stored in the pg_proc system catalog, multiplied by cpu_operator_cost to convert it to planner cost units.

The function distinguishes between startup costs (one-time initialization) and per-tuple costs (repeated for each input row), allowing for accurate modeling of function behavior in different query contexts.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context (may be NULL in some contexts)
- `funcid`: OID of the function for which to estimate execution cost
- `node`: Parse tree node representing the function call (FuncExpr, OpExpr, WindowFunc, etc.), or NULL if not available
- `cost`: QualCost structure to which the estimated function cost will be added

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - OidFunctionCall1
  - ReleaseSysCache
  - Form_pg_proc
  - SupportRequestCost
  - QualCost
- Called from (representative examples):
  - cost_windowagg
  - cost_qual_eval_walker
  - get_agg_clause_costs

## Notes and Other Information
The function performs error checking by verifying that the function OID exists in the system catalog. It properly manages system catalog cache resources by releasing the cached tuple after use. The cost estimation mechanism is extensible, allowing custom functions to provide their own costing logic through support functions. When using the default procost method, the cost is scaled by cpu_operator_cost to maintain consistency with other planner cost calculations.