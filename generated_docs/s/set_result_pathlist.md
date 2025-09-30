# set_result_pathlist

## Location
[src/backend/optimizer/path/allpaths.c:2966-2992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L2966-L2992)

## Overview
Builds the access path for an RTE_RESULT RTE (Result Table Entry), creating a single ResultScan path for relations that represent computed results rather than physical tables.

## Definition

```c
static void
set_result_pathlist(PlannerInfo *root, RelOptInfo *rel,
					RangeTblEntry *rte)
```
## Detailed Description
This function is responsible for constructing the pathlist for Result relations, which are used in PostgreSQL to represent computed results or values that don't come from scanning a physical table. The function creates exactly one path - a ResultScan path - since there's only one way to access a result relation. Unlike other relation types, Result relations don't support join-qual-parameterized paths, simplifying the path generation process.

The function first establishes size estimates for the relation, then determines any required outer relations due to LATERAL references in the target list, and finally creates and adds the appropriate ResultScan path to the relation's pathlist.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the Result relation for which paths are being generated
- : RangeTblEntry that describes this Result relation in the range table

## Dependencies
- Functions called/Symbols referenced:
  - [set_result_size_estimates](set_result_size_estimates.md)
  - [add_path](../a/add_path.md)
  - [create_resultscan_path](../c/create_resultscan_path.md)
- Called from (representative examples):
  - [set_rel_size](set_rel_size.md)

## Notes and Other Information
- This function is specifically designed for RTE_RESULT type relations
- No separate size estimation phase is needed since join-qual-parameterized paths are not supported
- The function handles LATERAL parameterization through the lateral_relids field
- [Result](../R/Result.md) relations typically represent computed values, constants, or function results rather than table scans

## Simplified Source

```c
static void
set_result_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    Relids required_outer;

    // Set size estimates for the result relation
    set_result_size_estimates(root, rel);

    // Handle LATERAL references as required parameterization
    // (Join clauses cannot be pushed into result scans)
    required_outer = rel->lateral_relids;

    // Create and add the result scan path
    add_path(rel, create_resultscan_path(root, rel, required_outer));
}
```