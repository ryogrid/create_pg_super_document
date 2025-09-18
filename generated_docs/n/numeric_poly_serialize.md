# numeric_poly_serialize

## Location
src/backend/utils/adt/numeric.c: 5697 - 5754

## Overview
The numeric_poly_serialize function serializes a PolyNumAggState structure into a bytea format for PostgreSQL's parallel aggregation framework, enabling transfer of partial aggregate states between processes.

## Definition
```c
Datum numeric_poly_serialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's parallel aggregation infrastructure, responsible for converting a PolyNumAggState structure into a portable bytea representation. The serialization process ensures platform independence by converting all numeric data (including 128-bit integers when available) into a standardized numeric format. This enables the transfer of partial aggregate states across different processes, potentially on different machines with different architectures.

The function serializes three key components: the count of values (N), the sum of values (sumX), and the sum of squares (sumX2). On platforms with 128-bit integer support, it converts the int128 values to numeric format for consistency. The serialized format uses PostgreSQL's standard binary protocol format, making it suitable for network transmission or storage.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - Argument 0: PolyNumAggState pointer (the state to serialize)

## Dependencies
- Functions called/Symbols referenced:
  - PolyNumAggState (data structure)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (context validation)
  - [StringInfoData](../S/StringInfoData.md) (buffer for serialization)
  - [NumericVar](../N/NumericVar.md) (temporary numeric variable)
  - init_var (numeric variable initialization)
  - [pq_begintypsend](../p/pq_begintypsend.md) (begin binary output)
  - [pq_sendint64](../p/pq_sendint64.md) (serialize int64 value)
  - [int128_to_numericvar](../i/int128_to_numericvar.md) (convert 128-bit int to numeric, when HAVE_INT128)
  - [accum_sum_final](../a/accum_sum_final.md) (finalize accumulator sum to numeric)
  - [numericvar_serialize](numericvar_serialize.md) (serialize numeric variable)
  - [pq_endtypsend](../p/pq_endtypsend.md) (end binary output)
  - [free_var](../f/free_var.md) (cleanup numeric variable)
  - PG_RETURN_BYTEA_P (return bytea result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function ensures platform portability by standardizing on numeric format regardless of underlying 128-bit integer support
- Part of PostgreSQL's parallel aggregation system for statistical functions requiring sum-of-squares calculations
- Uses PostgreSQL's binary protocol format for efficient and standardized serialization
- The function validates that it's called in an appropriate aggregate context
- Designed to work with numeric_poly_deserialize for complete serialization/deserialization cycle
- Memory management includes proper cleanup of temporary variables
- Critical for distributed query processing where partial results need to be transmitted between nodes