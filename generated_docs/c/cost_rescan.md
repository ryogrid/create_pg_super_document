# cost_rescan

## Location
[src/backend/optimizer/path/costsize.c:4528-4642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L4528-L4642)

## Overview
Estimates the costs of rescanning a path after the first scan, accounting for plan types that cache results or avoid redoing startup calculations.

## Definition

```c
static void
cost_rescan(PlannerInfo *root, Path *path,
			Cost *rescan_startup_cost,	/* output parameters */
			Cost *rescan_total_cost)
```
## Detailed Description
The  function calculates revised cost estimates for rescanning various PostgreSQL plan node types. Unlike initial scans, rescans can be significantly cheaper for certain plan types that cache intermediate results or avoid repeating expensive startup operations. The function analyzes the path type and applies specialized cost calculations based on the execution characteristics of each plan node type.

For most plan types, rescan costs equal original costs, but several key optimizations are modeled:
- **FunctionScan**: Function evaluation cost is paid only once (startup), rescans only pay run costs
- **HashJoin**: Single-batch joins avoid rebuilding the hash table on rescan
- **CteScan/WorkTableScan**: Materialized results in tuplestore require only tuple retrieval costs
- **Material/Sort**: Cheapest rescans with only cpu_operator_cost per tuple
- **Memoize**: Delegates to specialized  function

The function accounts for spilling to disk when materialized results exceed work_mem, adding sequential page read costs.

## Parameters / Member Variables
- : PlannerInfo context containing global planning information
- : The Path node whose rescan costs are being estimated
- : Output parameter for startup cost on rescan
- : Output parameter for total cost on rescan

## Dependencies
- Functions called/Symbols referenced:
  - [relation_byte_size](../r/relation_byte_size.md)
  - [cost_memoize_rescan](cost_memoize_rescan.md)
  - Cost (type)
  - HashPath (type)
  - MemoizePath (type)
- Called from (representative examples):
  - cost_qual_eval_context
  - [initial_cost_nestloop](../i/initial_cost_nestloop.md)

## Notes and Other Information
- This is a static function within costsize.c, used internally by the cost estimation system
- Does not model disk block caching effects, focuses on explicit result caching by executor nodes
- Spill-to-disk calculations use work_mem threshold and BLCKSZ page size
- Default case returns original path costs for plan types without special rescan optimizations
- Critical for nested loop join cost estimation where inner paths are rescanned multiple times