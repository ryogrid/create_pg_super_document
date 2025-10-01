# scalarineqsel

## Location
[src/backend/utils/adt/selfuncs.c:581-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L581-L732)

## Overview
Core selectivity estimation function for scalar inequality operators (<, <=, >, >=) that combines statistics from most-common-values (MCV) and histogram data to estimate the fraction of rows satisfying inequality conditions.

## Definition
```c
static double
scalarineqsel(PlannerInfo *root, Oid operator, bool isgt, bool iseq,
              Oid collation,
              VariableStatData *vardata, Datum constval, Oid consttype)
```

## Detailed Description
`scalarineqsel` is the core implementation function that powers the selectivity estimation for all four scalar inequality operators. It uses a sophisticated approach that combines multiple sources of statistical information:

1. **MCV Analysis**: Examines most-common-values to get exact selectivity for frequently occurring values
2. **Histogram Analysis**: Uses histogram bins to estimate selectivity for non-MCV values
3. **Special CTID Handling**: Provides specialized estimation for system column CTID based on physical table layout
4. **Fallback Strategy**: Uses default estimates when no statistics are available

The function handles the mathematical combination of MCV and histogram selectivity, accounting for the fact that histogram only covers non-null values not represented in MCV entries.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `operator`: OID of the inequality operator being evaluated
- `isgt`: Boolean flag indicating if this is a "greater than" type operator (> or >=)
- `iseq`: Boolean flag indicating if equality is included (<= or >=)
- `collation`: Collation OID for string comparison operations
- `vardata`: Statistical data structure for the column variable
- `constval`: The constant value being compared against
- `consttype`: Data type OID of the constant value

## Dependencies
- Functions called/Symbols referenced:
  - [mcv_selectivity](../m/mcv_selectivity.md)
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md)
  - [get_opcode](../g/get_opcode.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md)
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md)
  - CLAMP_PROBABILITY
  - DEFAULT_INEQ_SEL
- Called from (representative examples):
  - [scalarineqsel_wrapper](scalarineqsel_wrapper.md)
  - [mergejoinscansel](../m/mergejoinscansel.md)

## Notes and Other Information
- Works with any datatype supported by convert_to_scalar()
- Provides specialized CTID estimation using table physical layout knowledge
- Combines MCV and histogram selectivity using the formula: selec = mcv_selec + (1 - stanullfrac - sumcommon) * hist_selec
- Falls back to DEFAULT_INEQ_SEL (0.3333) when no statistics are available
- Results are always clamped to valid probability range [0.0, 1.0]
- The caller must ensure the clause is commuted so the variable is on the left side

## Simplified Source

```c
static double scalarineqsel(PlannerInfo *root, Oid operator, bool isgt, bool iseq,
                           Oid collation, VariableStatData *vardata,
                           Datum constval, Oid consttype) {
    Form_pg_statistic stats;
    FmgrInfo opproc;
    double mcv_selec, hist_selec, sumcommon, selec;

    // No statistics available
    if (!HeapTupleIsValid(vardata->statsTuple)) {
        // Special case: CTID comparison using table physical layout
        if (vardata->var && IsA(vardata->var, Var) &&
            ((Var *) vardata->var)->varattno == SelfItemPointerAttributeNumber) {

            if (vardata->rel->pages == 0) return 1.0;

            ItemPointer itemptr = (ItemPointer) DatumGetPointer(constval);
            double block = ItemPointerGetBlockNumberNoCheck(itemptr);
            double density = vardata->rel->tuples / (vardata->rel->pages - 0.5);

            // Adjust for position within page
            if (block >= vardata->rel->pages - 1) density *= 0.5;
            if (density > 0.0) {
                OffsetNumber offset = ItemPointerGetOffsetNumberNoCheck(itemptr);
                block += Min(offset / density, 1.0);
            }

            // Convert to selectivity
            selec = block / (vardata->rel->pages - 0.5);
            if (iseq == isgt && vardata->rel->tuples >= 1.0)
                selec -= (1.0 / vardata->rel->tuples);
            if (isgt) selec = 1.0 - selec;

            CLAMP_PROBABILITY(selec);
            return selec;
        }
        return DEFAULT_INEQ_SEL;
    }

    stats = (Form_pg_statistic) GETSTRUCT(vardata->statsTuple);
    fmgr_info(get_opcode(operator), &opproc);

    // Get selectivity from most-common-values
    mcv_selec = mcv_selectivity(vardata, &opproc, collation, constval, true, &sumcommon);

    // Get selectivity from histogram
    hist_selec = ineq_histogram_selectivity(root, vardata, operator, &opproc,
                                           isgt, iseq, collation, constval, consttype);

    // Combine MCV and histogram results
    // Histogram covers non-null values not in MCV
    selec = 1.0 - stats->stanullfrac - sumcommon;

    if (hist_selec >= 0.0)
        selec *= hist_selec;
    else
        selec *= 0.5;  // No histogram: assume half of non-MCV values match

    selec += mcv_selec;

    CLAMP_PROBABILITY(selec);
    return selec;
}
```