# var_eq_non_const

## Location
[src/backend/utils/adt/selfuncs.c:467-557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L467-L557)

## Overview
The var_eq_non_const function estimates selectivity for equality (and inequality) comparisons between a table column variable and a non-constant expression, such as another variable or computed value.

## Definition


## Detailed Description
The var_eq_non_const function handles selectivity estimation when comparing a column variable against non-constant expressions like other columns, function calls, or complex expressions. Since the comparison value is unknown at planning time, the function uses statistical assumptions to estimate selectivity:

1. **Unique constraint optimization**: For unique columns, assumes exactly one match using 1/tuple_count
2. **Uniform distribution assumption**: Assumes the unknown value is equally likely to match any distinct value in the column
3. **Most Common Value cross-check**: Ensures the estimated selectivity doesn't exceed the most frequent value's frequency
4. **Null fraction handling**: Accounts for NULL values in the selectivity calculation

The estimation formula is: (1.0 - nullfrac) / number_of_distinct_values, representing the probability that a randomly chosen non-null value will match the unknown comparison value.

## Parameters / Member Variables
- : Pointer to VariableStatData containing column statistics and metadata
- : OID of the comparison operator
- : Collation OID for string comparisons (used for context)
- : Parse tree node representing the non-constant expression being compared
- : Boolean indicating operator argument order (variable on left side)
- : Boolean flag to compute inequality selectivity instead of equality

## Dependencies
- Functions called/Symbols referenced:
  - get_variable_numdistinct
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - CLAMP_PROBABILITY
  - STATISTIC_KIND_MCV, ATTSTATSSLOT_NUMBERS (constants)
- Called from (representative examples):
  - [eqsel_internal](../e/eqsel_internal.md)

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:467-557
- This function is exported (non-static) and can be used by other estimation functions
- Returns selectivity values clamped between 0.0 and 1.0 using CLAMP_PROBABILITY
- Uses a uniform distribution assumption which may not always be accurate but provides a reasonable default
- The function's approach assumes the unknown comparison value has equal probability of matching any distinct value
- For inequality operations (negate=true), computes: 1.0 - equality_selectivity - nullfrac
- Cross-validates results against MCV statistics to prevent overestimation
- Falls back gracefully when detailed statistics are unavailable