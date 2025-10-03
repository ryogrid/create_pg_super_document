# get_variable_range

## Location
[src/backend/utils/adt/selfuncs.c:5963-6089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5963-L6089)

## Overview
Estimates the minimum and maximum values of a specified variable using statistical data from pg_statistic, with support for different sorting operators and collations.

## Definition

```c
static bool
get_variable_range(PlannerInfo *root, VariableStatData *vardata,
				   Oid sortop, Oid collation,
				   Datum *min, Datum *max)
```
## Detailed Description
This function attempts to determine the range (minimum and maximum values) of a database column or expression by analyzing available statistical information. It employs multiple strategies to find the most appropriate range data:

1. **Histogram Analysis**: First tries to use histogram data with the exact ordering operator requested, extracting the first and last values as min/max
2. **Alternative Histogram**: If no matching histogram exists, scans any available histogram to find extremal values according to the requested ordering
3. **Most Common Values (MCV) Analysis**: Examines MCV data for extreme values, with special logic to determine if MCVs alone represent a complete data distribution

The function includes security checks to ensure the user has permission to access the statistical data, and handles data type-specific operations like datum copying and comparison operators.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing query planning context
- `*vardata`: VariableStatData structure with statistical information and metadata about the variable
- `sortop`: Object identifier for the comparison operator to use (typically "<" for ascending order)
- `collation`: Collation to use for comparisons, important for text data types
- `*min`: Output parameter for the estimated minimum value
- `*max`: Output parameter for the estimated maximum value
## Dependencies
- Functions called/Symbols referenced:
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md) (security permission verification)
  - [get_opcode](get_opcode.md) (retrieve function OID for operator)
  - [get_typlenbyval](get_typlenbyval.md) (get type storage information)
  - [get_attstatsslot](get_attstatsslot.md) (retrieve statistical data slots)
  - [datumCopy](../d/datumCopy.md) (safely copy datum values)
  - [get_stats_slot_range](get_stats_slot_range.md) (scan statistics for range values)
  - [free_attstatsslot](../f/free_attstatsslot.md) (cleanup statistical data slots)
- Called from (representative examples):
  - [mergejoinscansel](../m/mergejoinscansel.md) (merge join selectivity estimation)

## Notes and Other Information
- Returns true if successful in finding range data, false if no statistical information is available
- The function includes disabled code (NOT_USED) for potentially using actual index probes to get precise min/max values, which was deemed too expensive for frequent use during join planning
- For MCV-only scenarios, the function checks if MCVs represent nearly the complete dataset (>99.999%) before using them for range estimation
- Security checks prevent unauthorized access to statistical data, ensuring the function respects database access controls
- The function handles different collations properly, which is crucial for text data where sorting order depends on locale
- Histogram data is preferred when available as it typically provides more accurate range information than MCV data alone
- The implementation carefully manages memory by copying datum values and freeing statistical slots after use

## Simplified Source

```c
static bool get_variable_range(PlannerInfo *root, VariableStatData *vardata,
                              Oid sortop, Oid collation,
                              Datum *min, Datum *max) {
    Datum tmin = 0, tmax = 0;
    bool have_data = false;
    int16 typLen;
    bool typByVal;
    AttStatsSlot sslot;

    // No statistics available - return failure
    if (!HeapTupleIsValid(vardata->statsTuple))
        return false;

    // Security check: ensure we can apply the sort operator
    if (!statistic_proc_security_check(vardata, get_opcode(sortop)))
        return false;

    get_typlenbyval(vardata->atttype, &typLen, &typByVal);

    // Strategy 1: Look for histogram with exact ordering operator
    if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_HISTOGRAM, sortop, ATTSTATSSLOT_VALUES)) {
        if (sslot.stacoll == collation && sslot.nvalues > 0) {
            // Use first and last histogram values as min/max
            tmin = datumCopy(sslot.values[0], typByVal, typLen);
            tmax = datumCopy(sslot.values[sslot.nvalues - 1], typByVal, typLen);
            have_data = true;
        }
        free_attstatsslot(&sslot);
    }

    // Strategy 2: Scan any histogram for extremal values with our ordering
    if (!have_data && get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES)) {
        get_stats_slot_range(&sslot, get_opcode(sortop), NULL, collation, typLen, typByVal, &tmin, &tmax, &have_data);
        free_attstatsslot(&sslot);
    }

    // Strategy 3: Check most-common-values for extreme values
    if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_MCV, InvalidOid,
                        have_data ? ATTSTATSSLOT_VALUES : (ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS))) {
        bool use_mcvs = have_data;

        // If no histogram, only use MCVs if they represent nearly complete dataset
        if (!have_data) {
            double sumcommon = 0.0, nullfrac;
            for (int i = 0; i < sslot.nnumbers; i++)
                sumcommon += sslot.numbers[i];
            nullfrac = ((Form_pg_statistic) GETSTRUCT(vardata->statsTuple))->stanullfrac;
            use_mcvs = (sumcommon + nullfrac > 0.99999);
        }

        if (use_mcvs)
            get_stats_slot_range(&sslot, get_opcode(sortop), NULL, collation, typLen, typByVal, &tmin, &tmax, &have_data);
        free_attstatsslot(&sslot);
    }

    *min = tmin;
    *max = tmax;
    return have_data;
}
```