# generic_restriction_selectivity

## Location
[src/backend/utils/adt/selfuncs.c:915-1041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L915-L1041)

## Overview
General-purpose selectivity estimation function for operators without specialized knowledge, using standard MCV and histogram statistics to provide reasonable selectivity estimates.

## Definition
```c
double
generic_restriction_selectivity(PlannerInfo *root, Oid oproid, Oid collation,
                                List *args, int varRelid,
                                double default_selectivity)
```

## Detailed Description
`generic_restriction_selectivity` serves as the fallback selectivity estimation function for operators that lack specialized knowledge but work on data types with standard statistics. The function implements a comprehensive approach:

1. **Pattern Recognition**: Identifies if the expression matches "VAR OP CONST" or "CONST OP VAR" patterns
2. **MCV Analysis**: Uses `mcv_selectivity` to get exact selectivity for most common values
3. **Histogram Estimation**: Applies `histogram_selectivity` for non-MCV population
4. **Adaptive Weighting**: For small histograms (10-100 entries), blends histogram and default estimates
5. **Null Handling**: Accounts for null fraction in final calculation
6. **Range Clamping**: Ensures results stay within reasonable probability bounds

The function assumes the operator is strict and immutable/stable, and treats the histogram as a reasonable random sample of non-MCV values.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `oproid`: OID of the operator for which selectivity is being estimated  
- `collation`: Collation OID for string comparison operations
- `args`: List of arguments to the operator (should be variable and constant)
- `varRelid`: Relation ID if expression involves a specific relation (0 for general case)
- `default_selectivity`: Fallback selectivity estimate to use when statistics are insufficient

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_variable](get_restriction_variable.md)
  - [mcv_selectivity](../m/mcv_selectivity.md)
  - [histogram_selectivity](../h/histogram_selectivity.md)
  - [get_opcode](get_opcode.md)
  - [fmgr_info](../f/fmgr_info.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [matchingsel](../m/matchingsel.md)
  - Various operator selectivity functions as generic fallback

## Notes and Other Information
- Returns default_selectivity if expression is not "VAR OP CONST" pattern
- Returns 0.0 for comparisons with NULL constants (assumes strict operators)
- Uses histogram with min_hist_size=10 and n_skip=1 parameters
- For histograms smaller than 100 entries, applies weighted combination with default estimate
- Clamps final results to [0.0001, 0.9999] range to avoid extreme estimates
- Combines MCV and histogram using formula: selec = mcvsel + (1 - nullfrac - mcvsum) * hist_selec
- Best suited for operators whose semantics are not strongly related to histogram ordering
- Requires standard PostgreSQL statistics collection (MCV and histogram) to be effective

## Simplified Source
```c
double
generic_restriction_selectivity(PlannerInfo *root, Oid oproid, Oid collation,
                                List *args, int varRelid,
                                double default_selectivity)
{
    double selec;
    VariableStatData vardata;
    Node *other;
    bool varonleft;

    // Check if expression matches "VAR OP CONST" or "CONST OP VAR" pattern
    if (!get_restriction_variable(root, args, varRelid,
                                  &vardata, &other, &varonleft))
        return default_selectivity;

    // Handle NULL constants - strict operators never return TRUE for NULL
    if (IsA(other, Const) && ((Const *) other)->constisnull) {
        ReleaseVariableStats(vardata);
        return 0.0;
    }

    if (IsA(other, Const)) {
        // Variable being compared to known constant
        Datum constval = ((Const *) other)->constvalue;
        FmgrInfo opproc;
        double mcvsum, mcvsel, nullfrac;
        int hist_size;

        fmgr_info(get_opcode(oproid), &opproc);

        // Calculate selectivity for most common values
        mcvsel = mcv_selectivity(&vardata, &opproc, collation,
                                 constval, varonleft, &mcvsum);

        // Get histogram-based selectivity for non-MCV population
        selec = histogram_selectivity(&vardata, &opproc, collation,
                                      constval, varonleft,
                                      10, 1, &hist_size);

        if (selec < 0) {
            // No usable histogram, use default
            selec = default_selectivity;
        } else if (hist_size < 100) {
            // Small histogram: blend with default estimate
            double hist_weight = hist_size / 100.0;
            selec = selec * hist_weight + default_selectivity * (1.0 - hist_weight);
        }

        // Clamp to reasonable probability range
        if (selec < 0.0001) selec = 0.0001;
        else if (selec > 0.9999) selec = 0.9999;

        // Get null fraction from statistics
        if (HeapTupleIsValid(vardata.statsTuple))
            nullfrac = ((Form_pg_statistic) GETSTRUCT(vardata.statsTuple))->stanullfrac;
        else
            nullfrac = 0.0;

        // Combine MCV and histogram results
        // selec covers non-null, non-MCV population
        selec *= 1.0 - nullfrac - mcvsum;
        selec += mcvsel;
    } else {
        // Non-constant comparison value - can't estimate
        selec = default_selectivity;
    }

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);
    return selec;
}
```