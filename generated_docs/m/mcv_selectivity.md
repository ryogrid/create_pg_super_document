# mcv_selectivity

## Location
[src/backend/utils/adt/selfuncs.c:733-823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L733-L823)

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
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - STATISTIC_KIND_MCV
  - ATTSTATSSLOT_VALUES
  - ATTSTATSSLOT_NUMBERS
- Called from (representative examples):
  - [scalarineqsel](../s/scalarineqsel.md)
  - [generic_restriction_selectivity](../g/generic_restriction_selectivity.md)
  - [patternsel_common](../p/patternsel_common.md)
  - [networksel](../n/networksel.md)

## Notes and Other Information
- Returns 0.0 for both selectivity and sumcommon if no MCV list is available
- Uses manual function invocation to handle potential NULL results gracefully
- Security check ensures operator function is safe to execute with column statistics
- MCV entries are stored with their frequency values, allowing precise selectivity calculation
- Total MCV coverage (sumcommon) is essential for combining with histogram selectivity
- Supports any operator that returns boolean values, not just standard comparison operators

## Simplified Source

```c
double
mcv_selectivity(VariableStatData *vardata, FmgrInfo *opproc, Oid collation,
                Datum constval, bool varonleft, double *sumcommonp)
{
    double mcv_selec = 0.0;
    double sumcommon = 0.0;
    AttStatsSlot sslot;

    // Check if MCV statistics are available and accessible
    if (HeapTupleIsValid(vardata->statsTuple) &&
        statistic_proc_security_check(vardata, opproc->fn_oid) &&
        get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_MCV, InvalidOid,
                         ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))
    {
        LOCAL_FCINFO(fcinfo, 2);

        // Initialize function call info for operator invocation
        InitFunctionCallInfoData(*fcinfo, opproc, 2, collation, NULL, NULL);
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].isnull = false;

        // Set up constant value based on operator orientation
        if (varonleft)
            fcinfo->args[1].value = constval;
        else
            fcinfo->args[0].value = constval;

        // Test each MCV entry against the predicate
        for (int i = 0; i < sslot.nvalues; i++)
        {
            // Set variable value for this MCV entry
            if (varonleft)
                fcinfo->args[0].value = sslot.values[i];
            else
                fcinfo->args[1].value = sslot.values[i];

            // Evaluate predicate and accumulate selectivity
            fcinfo->isnull = false;
            Datum result = FunctionCallInvoke(fcinfo);
            if (!fcinfo->isnull && DatumGetBool(result))
                mcv_selec += sslot.numbers[i];

            sumcommon += sslot.numbers[i];
        }

        free_attstatsslot(&sslot);
    }

    *sumcommonp = sumcommon;
    return mcv_selec;
}
```