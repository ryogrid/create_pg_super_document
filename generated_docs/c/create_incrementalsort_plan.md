# create_incrementalsort_plan

## Location
[src/backend/optimizer/plan/createplan.c:2215-2241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L2215-L2241)

## Overview
Creates an IncrementalSort plan node from an IncrementalSortPath, which optimizes sorting by leveraging existing partial ordering in the input data.

## Definition


## Detailed Description
The  function generates an IncrementalSort plan node, which is an optimized variant of sorting that takes advantage of input data that is already partially sorted. This function operates similarly to  but creates IncrementalSort nodes instead of regular Sort nodes.

IncrementalSort is a performance optimization introduced in PostgreSQL that can significantly reduce sorting costs when the input is already sorted on a prefix of the desired sort keys. Instead of sorting the entire dataset from scratch, it only needs to sort groups of rows that share the same values for the pre-sorted columns.

The function follows the same pattern as , including the CP_SMALL_TLIST optimization and special handling for child relations in inheritance scenarios.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information and context
- : IncrementalSortPath structure representing the chosen incremental sorting strategy
- : Integer bitmask controlling plan creation behavior, including optimization flags

## Dependencies
- Functions called/Symbols referenced:
  - : Recursively creates execution plans for subpaths
  - : Constructs IncrementalSort node from pathkey specifications
  - : Macro to check if a relation is a child relation
  - : Copies common path information to the plan node
- Called from (representative examples):
  - : Main plan creation dispatch function

## Notes and Other Information
- This is a static function, only accessible within the createplan.c compilation unit
- IncrementalSort leverages the  field from the path to determine how many leading columns are already sorted
- The optimization is particularly effective for ORDER BY clauses where the input is already sorted on a subset of the ordering columns
- Like regular Sort nodes, IncrementalSort nodes don't perform projection and pass through target list requirements
- The function uses the same CP_SMALL_TLIST optimization as create_sort_plan to minimize memory usage
- IncrementalSort was introduced as a performance enhancement to reduce sorting overhead in scenarios with partial pre-sorting