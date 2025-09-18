# int8_avg_deserialize

## Location
src/backend/utils/adt/numeric.c: 5944 - 5989

## Overview
Deserializes bytea back into PolyNumAggState structure, reconstructing the aggregate state from binary format.

## Definition
```c
Datum int8_avg_deserialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the counterpart to int8_avg_serialize, reconstructing a PolyNumAggState from its binary bytea representation. It uses PostgreSQL's standard recv-function infrastructure to parse the binary data and recreate the aggregate state. The function handles the cross-platform normalization by converting the standardized numeric format back to the platform-appropriate representation (either int128 or numeric sum accumulation).

The deserialized state can then be used in further aggregate operations, making this function essential for parallel query execution and distributed aggregation scenarios where aggregate states need to be transmitted between nodes.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to:
  - Arg 0: bytea containing the serialized state

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (structure type)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (validates aggregate context)
  - `PG_GETARG_BYTEA_PP` (extracts bytea argument)
  - `init_var` (initializes NumericVar)
  - [initReadOnlyStringInfo](initReadOnlyStringInfo.md) (initializes read buffer)
  - `makePolyNumAggStateCurrentContext` (creates new state)
  - [pq_getmsgint64](../p/pq_getmsgint64.md) (reads 64-bit integer from buffer)
  - [numericvar_deserialize](../n/numericvar_deserialize.md) (deserializes numeric variable)
  - [numericvar_to_int128](../n/numericvar_to_int128.md) (converts numeric to int128, when HAVE_INT128)
  - [accum_sum_add](../a/accum_sum_add.md) (adds to sum for numeric version)
  - [pq_getmsgend](../p/pq_getmsgend.md) (validates buffer end)
  - [free_var](../f/free_var.md) (cleans up NumericVar)
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate deserialization system)

## Notes and Other Information
- Essential counterpart to int8_avg_serialize for round-trip serialization
- Handles cross-platform compatibility by converting from standardized numeric format
- Uses conditional compilation with HAVE_INT128 to choose appropriate target format
- Must be called within aggregate context (enforced by AggCheckCallContext)
- Creates aggregate state in current memory context for proper lifecycle management
- Critical for parallel query execution and distributed processing scenarios