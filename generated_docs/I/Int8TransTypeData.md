# Int8TransTypeData

## Location
[src/backend/utils/adt/numeric.c:6666-6670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6666-L6670)

## Overview
Int8TransTypeData is a simple two-element structure used as the transition datatype for integer average and sum aggregates, storing both count and sum values for efficient computation.

## Definition


## Detailed Description
Int8TransTypeData serves as the transition datatype for avg(int2), avg(int4), sum(int2), and sum(int4) aggregate functions in PostgreSQL. This structure is particularly important for moving-aggregate mode operations where inverse transitions are required - by maintaining both count and sum, the system can properly handle the removal of values from the aggregate state. The structure uses 64-bit integers to prevent overflow issues that could occur with smaller integer types when accumulating many values.

## Parameters / Member Variables
- : Number of values that have been accumulated into the aggregate
- 00000     0: Sum of all accumulated values using 64-bit integer arithmetic

## Dependencies
- Functions called/Symbols referenced:
  - int64 (primitive 64-bit integer type)
- Called from (representative examples):
  - [int2_avg_accum](../i/int2_avg_accum.md)
  - [int4_avg_accum](../i/int4_avg_accum.md)
  - [int4_avg_combine](../i/int4_avg_combine.md)
  - [int2_avg_accum_inv](../i/int2_avg_accum_inv.md)
  - [int4_avg_accum_inv](../i/int4_avg_accum_inv.md)
  - [int8_avg](../i/int8_avg.md)
  - [int2int4_sum](../i/int2int4_sum.md)

## Notes and Other Information
This structure represents the internal state for integer averaging operations and is particularly crucial for moving-aggregate scenarios where values need to be both added and removed from the aggregate. The use of 64-bit integers ensures sufficient range to handle the accumulation of many 16-bit or 32-bit integer values without overflow. Unlike the more complex NumericAggState, this structure focuses specifically on simple count and sum operations for integer types, providing better performance for these common cases. The structure is used both for final sum calculations and as an intermediate step in average calculations where the final result is computed as sum/count.