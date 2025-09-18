# allow_star_schema_join

## Location
src/backend/optimizer/path/joinpath.c: 363 - 389

## Overview
Determines whether to override the param_source_rels heuristic to enable parameterized nested loop joins in star-schema scenarios where parameters come from multiple unjoined small tables.

## Definition


## Detailed Description
This function implements a special heuristic for star-schema query patterns where a large fact table needs to be joined with multiple smaller dimension tables. In typical scenarios, the optimizer's param_source_rels restriction prevents passing parameters down across joins unless there are join-order constraints that require it.

However, in star-schema patterns, the optimal plan often involves a parameterized path for the large table that requires parameters from multiple small dimension tables that are not directly joined to each other. The function identifies when this pattern applies and allows the restriction to be overridden.

The star-schema case is detected when the outer relation provides some (but not all) of the parameters needed by the inner relation's parameterized path. This indicates a scenario where stacking nested loops with small tables on the outside can produce an efficient execution plan.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer context (currently unused in the function)
- : Relids bitmap representing the set of base relations in the outer side of the join
- : Relids bitmap representing the set of relations that the inner path requires as parameters

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md)
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md)

## Notes and Other Information
This function is marked as static inline, indicating it's a small optimization function used internally within the joinpath.c module. The function enables more flexible join planning for data warehouse workloads that commonly use star-schema designs.

The override is specifically designed for nested loop joins where the traditional parameter passing restrictions would prevent optimal plan generation. Without this override, the optimizer might miss efficient execution strategies for complex star-schema queries involving multiple dimension tables.