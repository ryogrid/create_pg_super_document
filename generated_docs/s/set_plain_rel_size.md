# set_plain_rel_size

## Location
[src/backend/optimizer/path/allpaths.c:572-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L572-L588)

## Overview
Estimates the size and output characteristics for a plain base relation without subqueries or inheritance.

## Definition

```c
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```
## Detailed Description
This function establishes size estimates for plain base relations in PostgreSQL's query optimizer. It performs a two-step process: first checking the applicability of partial indexes (which can significantly impact size estimates), then computing the actual size estimates including row count and tuple width. The function is specifically designed for simple table relations that don't involve subqueries, inheritance, or other complex structures.

The order of operations is critical - partial index predicates must be evaluated first because partial unique indexes can affect the estimated number of distinct values and overall size calculations. After index predicate evaluation, the function delegates to  to perform the actual statistical analysis and size computation.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and planning context
- : RelOptInfo structure representing the relation being analyzed for size estimation
- : RangeTblEntry containing parse tree information about the relation

## Dependencies
- Functions called/Symbols referenced:
  - [check_index_predicates](../c/check_index_predicates.md) (evaluates partial index applicability)
  - [set_baserel_size_estimates](set_baserel_size_estimates.md) (computes actual size statistics)
- Called from:
  - [set_rel_size](set_rel_size.md) (main size estimation dispatcher)

## Notes and Other Information
- This is a static function within allpaths.c serving as a specialized handler for plain relations
- The function is part of the size estimation phase that precedes path generation in query planning
- Partial index evaluation must occur before size estimation because unique partial indexes affect distinctness calculations
- This function handles only the simplest case of base relations - other relation types have their own specialized size estimation functions
- The separation of index predicate checking and size estimation allows for proper dependency handling in the optimizer

## Simplified Source

```c
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    // Check partial index applicability first - affects size estimates
    // Must be done before size estimation because partial unique indexes
    // can change distinctness calculations
    check_index_predicates(root, rel);

    // Compute size estimates using standard base relation logic
    set_baserel_size_estimates(root, rel);
}
```