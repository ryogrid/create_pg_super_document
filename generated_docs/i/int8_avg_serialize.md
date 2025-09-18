# int8_avg_serialize

## Location
src/backend/utils/adt/numeric.c: 5895 - 5943

## Overview
Serializes PolyNumAggState into bytea format using PostgreSQL's standard recv-function infrastructure for network transmission or storage.

## Definition
```c
Datum int8_avg_serialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts the internal PolyNumAggState structure into a binary bytea format that can be transmitted over the network or stored persistently. It ensures cross-platform compatibility by normalizing the internal representation - specifically converting int128 values to numeric format so that the serialized state has a consistent format regardless of whether the platform supports 128-bit integers.

The serialization includes the count (N) and the sum (sumX) in a standardized format that can be deserialized on any PostgreSQL instance. This is essential for parallel query execution across different nodes that may have different hardware capabilities.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to:
  - Arg 0: PolyNumAggState pointer (the state to serialize)

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (structure type)
  - `AggCheckCallContext` (validates aggregate context)
  - `init_var` (initializes NumericVar)
  - `pq_begintypsend` (begins binary output buffer)
  - `pq_sendint64` (sends 64-bit integer)
  - `int128_to_numericvar` (converts int128 to numeric, when HAVE_INT128)
  - `accum_sum_final` (finalizes sum for numeric version)
  - `numericvar_serialize` (serializes numeric variable)
  - `pq_endtypsend` (finalizes binary output)
  - `free_var` (cleans up NumericVar)
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate serialization system)

## Notes and Other Information
- Critical for parallel query execution and distributed aggregation
- Ensures cross-platform compatibility by normalizing int128 to numeric format
- Uses PostgreSQL's standard binary serialization protocol
- Must be called within aggregate context (enforced by AggCheckCallContext)
- Returns bytea that can be transmitted over network or stored persistently
- Paired with int8_avg_deserialize for round-trip serialization