# set_foreign_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:926-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L926-L943)

## Overview
Builds access paths for a foreign table by delegating to the Foreign Data Wrapper's path generation function.

## Definition

```c
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function serves as a simple dispatcher that delegates the responsibility of generating access paths for foreign tables to the Foreign Data Wrapper (FDW). Unlike other path generation functions that implement complex logic for creating various path types, this function takes a minimalist approach by relying entirely on the FDW's specialized knowledge of how to best access the foreign data source.

The FDW's GetForeignPaths callback is expected to analyze the query context and generate one or more Path structures that represent different ways to scan the foreign table. This might include basic foreign scans, parameterized scans for join conditions, or specialized paths that take advantage of remote sorting, filtering, or aggregation capabilities.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner information and query context
- : RelOptInfo structure representing the foreign relation, will have paths added to its pathlist
- : RangeTblEntry containing information about the foreign table and its access requirements

## Dependencies
- Functions called/Symbols referenced:
  - GetForeignPaths (FDW callback to generate access paths for the foreign table)

- Called from (representative examples):
  - [set_rel_pathlist](set_rel_pathlist.md) (main relation path list builder)

## Notes and Other Information
- This function is static and only used within allpaths.c
- The function is intentionally minimal, acting as a pure delegation wrapper
- The FDW has complete control over what types of paths are generated
- Unlike native table path generation, no fallback or default paths are provided
- The FDW's GetForeignPaths callback is responsible for adding paths to rel->pathlist
- The function assumes the FDW will generate at least one valid path for the foreign table
- This design allows FDWs maximum flexibility in implementing optimization strategies specific to their data sources

## Simplified Source

```c
static void set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte) {
    // Delegate path generation to the Foreign Data Wrapper
    rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
}
```