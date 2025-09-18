# numeric_serialize

## Location
[src/backend/utils/adt/numeric.c:5330-5385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5330-L5385)

## Overview
Serializes NumericAggState for numeric aggregates that require sumX2 (sum of squares), converting the complete aggregate state including second-moment calculations into bytea format.

## Definition
```c
Datum numeric_serialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the full-featured counterpart to numeric_avg_serialize, designed for numeric aggregates that require both the sum of values (sumX) and the sum of squares (sumX2). This includes aggregates like variance and standard deviation calculations that need second-moment statistics.

The serialization process includes all state information from NumericAggState: count of values (N), sum of values (sumX), sum of squares (sumX2), scale information, and counts for special numeric values. The inclusion of sumX2 distinguishes this function from numeric_avg_serialize and makes it suitable for more complex statistical aggregates.

Like its simpler counterpart, this function ensures proper aggregate context validation and uses PostgreSQL's standard binary serialization protocol for cross-platform compatibility.

## Parameters / Member Variables
- `fcinfo`: Function call information containing the NumericAggState pointer as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate context
  - init_var: Initializes temporary NumericVar
  - [pq_begintypsend](../p/pq_begintypsend.md): Starts binary serialization buffer
  - [pq_sendint64](../p/pq_sendint64.md): Serializes 64-bit integers (N, maxScaleCount, NaNcount, pInfcount, nInfcount)
  - [accum_sum_final](../a/accum_sum_final.md): Finalizes accumulated sums (called twice for sumX and sumX2)
  - [numericvar_serialize](numericvar_serialize.md): Serializes numeric values (called twice for sumX and sumX2)
  - [pq_sendint32](../p/pq_sendint32.md): Serializes 32-bit integer (maxScale)
  - [pq_endtypsend](../p/pq_endtypsend.md): Completes serialization and returns bytea result
  - [free_var](../f/free_var.md): Cleans up temporary variable
  - PG_RETURN_BYTEA_P: Returns the serialized bytea result
- Called from (representative examples):
  - Not directly referenced by other symbols (used by aggregate framework)

## Notes and Other Information
- Designed for aggregates requiring sumX2 (sum of squares) such as variance/stddev calculations
- Serializes both sumX and sumX2, unlike numeric_avg_serialize which only handles sumX
- Essential component of PostgreSQL's parallel aggregation system for statistical functions
- Uses the same binary protocol as other serialization functions for consistency
- Includes comprehensive state preservation for complex numeric aggregates
- Part of the infrastructure supporting distributed computation of statistical aggregates