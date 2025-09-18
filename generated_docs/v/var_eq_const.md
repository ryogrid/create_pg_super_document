# var_eq_const

## Location
src/backend/utils/adt/selfuncs.c: 296 - 466

## Overview
The var_eq_const function calculates selectivity estimates for equality (and inequality) comparisons between a table column variable and a constant value, utilizing column statistics when available.

## Definition


## Detailed Description
The var_eq_const function is a sophisticated selectivity estimator that leverages PostgreSQL's column statistics to provide accurate estimates for variable-constant comparisons. The function employs multiple estimation strategies based on available statistical information:

1. **NULL handling**: Returns 0.0 selectivity for NULL constants (assuming strict operators)
2. **Unique constraint optimization**: For unique columns, uses 1/tuple_count for exact selectivity
3. **Most Common Values (MCV) analysis**: Searches the MCV list to find exact matches with the constant
4. **Statistical distribution estimation**: For non-MCV values, estimates selectivity based on remaining distinct values
5. **Fallback estimation**: Uses uniform distribution assumption when no statistics are available

The function also handles inequality operations through the negate parameter, computing '1.0 - equality_selectivity - nullfrac' for <> operations.

## Parameters / Member Variables
- : Pointer to VariableStatData containing column statistics and metadata
- : OID of the comparison operator
- : Collation OID for string comparisons
- : The constant value being compared against
- : Boolean indicating if the constant is NULL
- : Boolean indicating operator argument order (variable on left side)
- : Boolean flag to compute inequality selectivity instead of equality

## Dependencies
- Functions called/Symbols referenced:
  - get_opcode
  - statistic_proc_security_check
  - get_attstatsslot
  - fmgr_info
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - get_variable_numdistinct
  - free_attstatsslot
  - CLAMP_PROBABILITY
  - STATISTIC_KIND_MCV, ATTSTATSSLOT_VALUES, ATTSTATSSLOT_NUMBERS (constants)
- Called from (representative examples):
  - eqsel_internal
  - boolvarsel
  - patternsel_common
  - prefix_selectivity

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:296-466
- This function is exported (non-static) and can be used by other estimation functions
- Returns selectivity values clamped between 0.0 and 1.0 using CLAMP_PROBABILITY
- Performs actual function calls to test equality using the provided operator
- Handles collation-sensitive comparisons properly
- Uses security checks before accessing detailed column statistics
- Optimizes performance by setting up function call info structures once and reusing them
- Falls back gracefully when statistics are unavailable or insufficient