# set_tablesample_rel_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:854-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L854-L901)

## Overview
Builds access paths for a sampled relation, creating a SampleScan path and optionally wrapping it in a Materialize node for sampling methods that don't support repeatable scans.

## Definition

```c
static void
set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function constructs the access path list for a relation that uses table sampling. It creates a SampleScan path as the primary access method for the sampled relation. The function handles a key constraint of some sampling methods: those that don't support repeatable scans across multiple executions.

When a sampling method doesn't support repeatable scans and there's a risk of multiple scans (detected by checking if the query involves joins or is within a subquery), the function wraps the SampleScan path in a Materialize node. This materialization ensures that the sampled data is consistent across multiple accesses within the same query execution.

The function doesn't support pushing join clauses into the sampling scan's quals, but does handle required parameterization from LATERAL references in the target list or TABLESAMPLE arguments.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and query information
- : RelOptInfo structure for the sampled relation, will have paths added to its pathlist
- : RangeTblEntry containing the table sampling clause and relation information

## Dependencies
- Functions called/Symbols referenced:
  - [create_samplescan_path](../c/create_samplescan_path.md) (creates the basic sample scan path)
  - [bms_membership](../b/bms_membership.md) (checks bitmap set membership characteristics)
  - BMS_SINGLETON (constant for singleton bitmap set)
  - [GetTsmRoutine](../G/GetTsmRoutine.md) (retrieves table sampling method routines)
  - [create_material_path](../c/create_material_path.md) (wraps path in materialization)
  - [add_path](../a/add_path.md) (adds path to relation's path list)

- Called from (representative examples):
  - [set_rel_pathlist](set_rel_pathlist.md) (main relation path list builder)

## Notes and Other Information
- This function is static and only used within allpaths.c
- Join clauses cannot be pushed down into sampling scan quals
- LATERAL parameterization is supported through required_outer handling
- Materialization is added as a safety measure for non-repeatable sampling methods
- The check for GetTsmRoutine's repeatable_across_scans is performed last due to its relative expense
- Risk of multiple scans is detected by checking query level and counting relations in all_query_rels
- Currently only considers SampleScan paths; no other path types are generated for sampled relations