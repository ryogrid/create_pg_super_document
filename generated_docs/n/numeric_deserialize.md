# numeric_deserialize

## Location
src/backend/utils/adt/numeric.c: 5386 - 5446

## Overview
Deserializes a bytea back into NumericAggState for numeric aggregates that require sumX2 (sum of squares), reconstructing the complete aggregate state for statistical calculations.

## Definition
```c
Datum numeric_deserialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the counterpart to numeric_serialize, performing deserialization of NumericAggState for aggregates that require both sumX and sumX2. It's essential for statistical aggregates like variance and standard deviation that need second-moment calculations in parallel processing environments.

The function carefully reconstructs all components of the aggregate state in the exact order they were serialized: count of values, sum of values, sum of squares, scale information, and special value counts. The deserialization of both sumX and sumX2 distinguishes this from numeric_avg_deserialize and enables complex statistical computations to continue across process boundaries.

The reconstructed state is allocated in the current memory context and can be used for further aggregate computation or combination with other partial states in parallel processing scenarios.

## Parameters / Member Variables
- `fcinfo`: Function call information containing the serialized bytea as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate context
  - PG_GETARG_BYTEA_PP: Retrieves the serialized bytea argument
  - init_var: Initializes temporary NumericVar
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md): Sets up buffer for reading binary data
  - [makeNumericAggStateCurrentContext](../m/makeNumericAggStateCurrentContext.md): Creates new NumericAggState in current context
  - [pq_getmsgint64](../p/pq_getmsgint64.md): Deserializes 64-bit integers (N, maxScaleCount, NaNcount, pInfcount, nInfcount)
  - [numericvar_deserialize](numericvar_deserialize.md): Deserializes numeric values (called twice for sumX and sumX2)
  - [accum_sum_add](../a/accum_sum_add.md): Adds deserialized sums to the aggregate state (called twice)
  - [pq_getmsgint](../p/pq_getmsgint.md): Deserializes 32-bit integer (maxScale)
  - [pq_getmsgend](../p/pq_getmsgend.md): Validates end of message buffer
  - [free_var](../f/free_var.md): Cleans up temporary variable
- Called from (representative examples):
  - Not directly referenced by other symbols (used by aggregate framework)

## Notes and Other Information
- Companion function to numeric_serialize for parallel processing of statistical aggregates
- Handles both sumX and sumX2, unlike numeric_avg_deserialize which only processes sumX
- Critical for variance, standard deviation, and other second-moment statistical calculations
- Part of PostgreSQL's parallel aggregation infrastructure for complex numeric computations
- Uses read-only StringInfo for efficient binary data parsing
- Includes comprehensive validation and proper memory context management
- Enables distributed computation of sophisticated statistical functions