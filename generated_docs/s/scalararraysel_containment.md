# scalararraysel_containment

## Location
src/backend/utils/adt/array_selfuncs.c: 81 - 240

## Overview
Estimates selectivity of ScalarArrayOpExpr operations via array containment analysis, converting expressions like 'const =/<> ANY/ALL (array_var)' into equivalent array containment operations.

## Definition


## Detailed Description
This function provides selectivity estimation for scalar array operations by transforming them into array containment operations. It handles expressions of the form 'const =/<> ANY/ALL (array_var)' by treating them as array containment operations like 'array_var op ARRAY[const]'.

The function distinguishes between two cases:
- For = ANY operations: estimates as 'var @> ARRAY[const]' (contains)
- For = ALL operations: estimates as 'var <@ ARRAY[const]' (contained by)

For inequality operators (<>), the function swaps ANY/ALL semantics and inverts the final result. The estimation relies on most-common-elements (MCE) statistics and distinct-element count histograms when available.

## Parameters
- : PlannerInfo containing query planning context
- : Left operand node (must be a constant)
- : Right operand node (must be a variable)
- : OID of the array element type
- : true for = operator, false for <> operator
- : true for ANY semantics, false for ALL semantics
- : Variable relation ID for statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - examine_variable
  - ReleaseVariableStats
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - statistic_proc_security_check
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [mcelem_array_contain_overlap_selec](../m/mcelem_array_contain_overlap_selec.md)
  - [mcelem_array_contained_selec](../m/mcelem_array_contained_selec.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - CLAMP_PROBABILITY
- Called from:
  - scalararraysel (src/backend/utils/adt/selfuncs.c:1878)

## Notes and Other Information
- Returns selectivity value between 0 and 1, or -1 if estimation fails
- Requires the left operand to be a constant and right operand to be a variable
- Uses array element statistics (MCELEM and DECHIST) when available
- Adjusts for null fraction in the statistics
- Part of PostgreSQL's cost-based optimizer selectivity estimation framework