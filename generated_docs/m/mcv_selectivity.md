# mcv_selectivity

## Location
src/backend/utils/adt/selfuncs.c: 733 - 823

## Overview
Examines the Most Common Values (MCV) list to compute selectivity estimates for predicates involving the most frequently occurring column values.

## Definition
```c
double
mcv_selectivity(VariableStatData *vardata, FmgrInfo *opproc, Oid collation,
                Datum constval, bool varonleft,
                double *sumcommonp)
```

## Detailed Description
`mcv_selectivity` analyzes the Most Common Values statistics to determine what fraction of the MCV population satisfies a given predicate condition. It works by:

1. **MCV List Access**: Retrieves the MCV list from column statistics using `get_attstatsslot`
2. **Predicate Evaluation**: Tests each MCV entry against the constant using the provided operator function
3. **Selectivity Computation**: Accumulates the frequencies of matching MCV entries
4. **Total Coverage**: Calculates the total fraction of the column population represented by all MCV entries

The function supports both `VAR OP CONST` and `CONST OP VAR` forms based on the `varonleft` parameter, making it flexible for commuted predicates. It handles NULL results gracefully and works with any boolean-returning predicate operator.

## Parameters / Member Variables
- `vardata`: Statistical data structure containing column statistics including MCV list
- `opproc`: Function manager info for the comparison operator to be applied
- `collation`: Collation OID for string comparison operations
- `constval`: The constant value being compared against MCV entries
- `varonleft`: Boolean indicating whether variable is on left side of operator (affects argument order)
- `sumcommonp`: Output parameter that receives the total fraction of population covered by MCV entries

## Dependencies
- Functions called/Symbols referenced:
  - statistic_proc_security_check
  - get_attstatsslot
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - free_attstatsslot
  - STATISTIC_KIND_MCV
  - ATTSTATSSLOT_VALUES
  - ATTSTATSSLOT_NUMBERS
- Called from (representative examples):
  - scalarineqsel
  - generic_restriction_selectivity
  - patternsel_common
  - networksel

## Notes and Other Information
- Returns 0.0 for both selectivity and sumcommon if no MCV list is available
- Uses manual function invocation to handle potential NULL results gracefully
- Security check ensures operator function is safe to execute with column statistics
- MCV entries are stored with their frequency values, allowing precise selectivity calculation
- Total MCV coverage (sumcommon) is essential for combining with histogram selectivity
- Supports any operator that returns boolean values, not just standard comparison operators