# set_foreign_size

## Location
[src/backend/optimizer/path/allpaths.c:902-925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L902-L925)

## Overview
Sets size estimates for a foreign table by calling the Foreign Data Wrapper's size estimation routine and ensuring the resulting estimates are reasonable.

## Definition

```c
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function establishes size estimates for foreign tables by coordinating with the Foreign Data Wrapper (FDW). It first sets baseline estimates using PostgreSQL's standard estimation logic, then allows the FDW to provide more accurate estimates based on its knowledge of the foreign data source.

The function includes important sanity checks to prevent unreasonable estimates that could negatively impact query planning. It ensures that the row estimate is never zero (which could cause division errors) and that the tuple count is at least as large as the row estimate, handling cases where pg_class.reltuples might contain -1 or other invalid values.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planner information and context
- `*rel`: RelOptInfo structure representing the foreign relation, will be updated with size estimates
- `*rte`: RangeTblEntry containing information about the foreign table being sized
## Dependencies
- Functions called/Symbols referenced:
  - [set_foreign_size_estimates](set_foreign_size_estimates.md) (sets initial size estimates using standard logic)
  - GetForeignRelSize (FDW callback to adjust size estimates)
  - [clamp_row_est](../c/clamp_row_est.md) (ensures row estimate is not zero)
  - Max (macro to get maximum of two values)

- Called from (representative examples):
  - [set_rel_size](set_rel_size.md) (main relation sizing dispatcher)

## Notes and Other Information
- This function is static and only used within allpaths.c
- The FDW's GetForeignRelSize callback can override the initial estimates with more accurate values
- Row estimates are clamped to prevent zero values that could cause planning errors
- The tuples estimate is enforced to be at least as large as the rows estimate for consistency
- Handles the case where pg_class.reltuples contains -1 (unknown) by ensuring a minimum reasonable value
- The FDW has full control over size estimates but they are subject to final sanity checks

## Simplified Source

```c
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    // Set initial size estimates using standard PostgreSQL logic
    set_foreign_size_estimates(root, rel);

    // Let FDW provide more accurate estimates based on foreign data source
    rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

    // Ensure row estimate is never zero to prevent division errors
    rel->rows = clamp_row_est(rel->rows);

    // Ensure tuples count is at least as large as rows for consistency
    // This handles cases where pg_class.reltuples contains -1
    rel->tuples = Max(rel->tuples, rel->rows);
}
```