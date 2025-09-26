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
  - mcv_selectivity
  - ineq_histogram_selectivity
  - get_opcode
  - fmgr_info
  - ItemPointerGetBlockNumberNoCheck
  - ItemPointerGetOffsetNumberNoCheck
  - CLAMP_PROBABILITY
  - DEFAULT_INEQ_SEL
- Called from (representative examples):
  - scalarineqsel_wrapper
  - mergejoinscansel

## Notes and Other Information
- Works with any datatype supported by convert_to_scalar()
- Provides specialized CTID estimation using table physical layout knowledge
- Combines MCV and histogram selectivity using the formula: selec = mcv_selec + (1 - stanullfrac - sumcommon) * hist_selec
- Falls back to DEFAULT_INEQ_SEL (0.3333) when no statistics are available
- Results are always clamped to valid probability range [0.0, 1.0]
- The caller must ensure the clause is commuted so the variable is on the left side