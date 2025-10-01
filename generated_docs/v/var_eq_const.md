# var_eq_const

## Location
[src/backend/utils/adt/selfuncs.c:296-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L296-L466)

## Overview
The var_eq_const function calculates selectivity estimates for equality (and inequality) comparisons between a table column variable and a constant value, utilizing column statistics when available.

## Definition

```c
struct just once.
			 * Using FunctionCallInvoke directly also avoids failure if the
			 * eqproc returns NULL, though really equality functions should
			 * never do that.
			 */
			InitFunctionCallInfoData(*fcinfo, &eqproc, 2, collation,
									 NULL, NULL);
```
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
  - [get_opcode](../g/get_opcode.md)
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [fmgr_info](../f/fmgr_info.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - [get_variable_numdistinct](../g/get_variable_numdistinct.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - CLAMP_PROBABILITY
  - STATISTIC_KIND_MCV, ATTSTATSSLOT_VALUES, ATTSTATSSLOT_NUMBERS (constants)
- Called from (representative examples):
  - [eqsel_internal](../e/eqsel_internal.md)
  - [boolvarsel](../b/boolvarsel.md)
  - [patternsel_common](../p/patternsel_common.md)
  - [prefix_selectivity](../p/prefix_selectivity.md)

## Notes and Other Information
- Located in src/backend/utils/adt/selfuncs.c:296-466
- This function is exported (non-static) and can be used by other estimation functions
- Returns selectivity values clamped between 0.0 and 1.0 using CLAMP_PROBABILITY
- Performs actual function calls to test equality using the provided operator
- Handles collation-sensitive comparisons properly
- Uses security checks before accessing detailed column statistics
- Optimizes performance by setting up function call info structures once and reusing them
- Falls back gracefully when statistics are unavailable or insufficient

## Simplified Source

```c
double
var_eq_const(VariableStatData *vardata, Oid oproid, Oid collation,
             Datum constval, bool constisnull, bool varonleft, bool negate)
{
    double selec;
    double nullfrac = 0.0;

    // NULL constants always return 0 selectivity
    if (constisnull)
        return 0.0;

    // Get null fraction from statistics if available
    if (HeapTupleIsValid(vardata->statsTuple)) {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);
        nullfrac = stats->stanullfrac;
    }

    // For unique columns: selectivity = 1/tuple_count
    if (vardata->isunique && vardata->rel && vardata->rel->tuples >= 1.0) {
        selec = 1.0 / vardata->rel->tuples;
    }
    // Use detailed statistics if available and secure
    else if (HeapTupleIsValid(vardata->statsTuple) &&
             statistic_proc_security_check(vardata, get_opcode(oproid))) {

        AttStatsSlot sslot;
        bool match = false;

        // Check if constant matches any Most Common Value
        if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_MCV,
                            InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)) {

            // Setup function call to test equality with each MCV
            FmgrInfo eqproc;
            fmgr_info(get_opcode(oproid), &eqproc);

            for (int i = 0; i < sslot.nvalues; i++) {
                // Test if constant equals this common value
                if (values_are_equal(constval, sslot.values[i], &eqproc, varonleft, collation)) {
                    match = true;
                    selec = sslot.numbers[i]; // Use exact MCV frequency
                    break;
                }
            }
        }

        if (!match) {
            // Constant not in MCV list - estimate from remaining values
            double sumcommon = 0.0;
            for (int i = 0; i < sslot.nnumbers; i++)
                sumcommon += sslot.numbers[i];

            // Remaining selectivity divided among other distinct values
            selec = 1.0 - sumcommon - nullfrac;
            double otherdistinct = get_variable_numdistinct(vardata, NULL) - sslot.nnumbers;
            if (otherdistinct > 1)
                selec /= otherdistinct;

            // Cap at least common MCV frequency
            if (sslot.nnumbers > 0 && selec > sslot.numbers[sslot.nnumbers - 1])
                selec = sslot.numbers[sslot.nnumbers - 1];
        }

        free_attstatsslot(&sslot);
    }
    else {
        // No statistics - assume uniform distribution
        selec = 1.0 / get_variable_numdistinct(vardata, NULL);
    }

    // For inequality (!= operator), compute complement
    if (negate)
        selec = 1.0 - selec - nullfrac;

    CLAMP_PROBABILITY(selec);
    return selec;
}
```