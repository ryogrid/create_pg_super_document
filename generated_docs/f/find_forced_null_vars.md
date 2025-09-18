# find_forced_null_vars

## Location
src/backend/optimizer/util/clauses.c: 1916 - 1976

## Overview
Determines which variables must be NULL for a given clause to return TRUE, serving as the complement to find_nonnullable_vars.

## Definition
List *find_forced_null_vars(Node *node)

## Detailed Description
This function identifies level-zero Vars that must be NULL for the given clause to return TRUE. The analysis is intentionally simplistic and conservative, primarily detecting simple "var IS NULL" tests at the top level. The function uses a straightforward approach that handles single-clause cases through a subroutine and processes AND-conditions by recursively examining each arm.

The function does not attempt to analyze complex OR expressions or NOT expressions, focusing on the most common and straightforward cases where NULL constraints can be reliably identified. Like find_nonnullable_vars, it returns results as a multibitmapset structure where List indexes correspond to relation IDs (varnos) and the per-relation Bitmapsets hold variable attribute numbers offset by FirstLowInvalidHeapAttributeNumber.

For implicit-AND lists, the function accumulates forced-null variables from each arm since any arm producing FALSE-or-NULL will make the overall result FALSE-or-NULL. The function is designed to be conservative, preferring to miss some cases rather than incorrectly identify forced-null constraints.

## Parameters / Member Variables
- node: The expression node to analyze for forced-null variable constraints

## Dependencies
- Functions called/Symbols referenced:
  - [find_forced_null_var](find_forced_null_var.md)
  - mbms_add_member
  - mbms_add_members
  - [find_forced_null_vars](find_forced_null_vars.md) (recursive calls)
- Called from (representative examples):
  - [reduce_outer_joins_pass2](../r/reduce_outer_joins_pass2.md)
  - [find_forced_null_vars](find_forced_null_vars.md) (recursive)
  - WindowFuncLists

## Notes and Other Information
- The function intentionally avoids analyzing OR and NOT cases for simplicity
- It is designed to err on the side of conservatism in its analysis
- The function is particularly useful for outer join reduction optimizations
- Results are returned in the same multibitmapset format as find_nonnullable_vars for consistency
- The analysis is limited to top-level NULL tests to maintain reliability and performance