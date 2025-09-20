# set_tablesample_rel_size

## Location
[src/backend/optimizer/path/allpaths.c:814-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L814-L853)

## Overview
Sets size estimates for a sampled relation by calling the table sampling method's estimation function to determine pages and tuples to be processed.

## Definition

```c
static void
set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function estimates the size characteristics of a relation that will be accessed through table sampling. It coordinates with the table sampling method (TSM) to obtain realistic estimates of how many pages will be read and how many tuples will be returned. The function first checks any partial indexes for applicability since they can affect size estimates, then calls the sampling method's estimation function, and finally updates the relation's size estimates accordingly.

The function assumes that only a SampleScan path will be considered for the sampled relation, so it directly overwrites the relation's pages and tuples estimates. If multiple path types were to be considered for sampled relations in the future, additional complexity would be needed.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner information and context
- : RelOptInfo structure representing the relation being sized, will be updated with new estimates  
- : RangeTblEntry containing the table sampling clause and related information

## Dependencies
- Functions called/Symbols referenced:
  - [TableSampleClause](../T/TableSampleClause.md) (struct type for sampling clause)
  - TsmRoutine (struct type for table sampling method routines)
  - [check_index_predicates](../c/check_index_predicates.md) (checks partial index applicability)
  - [GetTsmRoutine](../G/GetTsmRoutine.md) (retrieves table sampling method routines)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md) (finalizes relation size estimates)

- Called from (representative examples):
  - [set_rel_size](set_rel_size.md) (main relation sizing dispatcher)

## Notes and Other Information
- This function is static and only used within allpaths.c
- The sampling method's SampleScanGetSampleSize function is assumed to return reasonable values
- Partial index checking is performed first since it can impact size estimates
- Currently designed assuming only SampleScan paths will be used for sampled relations
- The function directly overwrites rel->pages and rel->tuples with the sampling estimates