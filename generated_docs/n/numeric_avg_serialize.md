# numeric_avg_serialize

## Location
[src/backend/utils/adt/numeric.c:5220-5271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5220-L5271)

## Overview
Serializes NumericAggState for numeric aggregates that don't require sumX2, converting the aggregate state into a bytea format for storage or transmission.

## Definition


## Detailed Description
This function is part of PostgreSQL's numeric aggregation framework, specifically designed to serialize the state of numeric averaging aggregates that do not require the sum of squares (sumX2). The function converts a NumericAggState structure into a binary format using PostgreSQL's type send/receive protocol.

The serialization process includes all essential state information: the count of values (N), the sum of values (sumX), scale information (maxScale and maxScaleCount), and counts for special numeric values (NaN, positive infinity, negative infinity). This allows the aggregate state to be preserved across parallel processing boundaries or stored for later use.

The function ensures it's only called within an aggregate context and uses a temporary NumericVar to finalize the sum before serialization.

## Parameters / Member Variables
- : Function call information containing the NumericAggState pointer as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate context
  - init_var: Initializes temporary NumericVar
  - [pq_begintypsend](../p/pq_begintypsend.md): Starts binary serialization buffer
  - [pq_sendint64](../p/pq_sendint64.md): Serializes 64-bit integers (N, maxScaleCount, NaNcount, pInfcount, nInfcount)
  - [accum_sum_final](../a/accum_sum_final.md): Finalizes the accumulated sum
  - [numericvar_serialize](numericvar_serialize.md): Serializes the numeric sum value
  - [pq_sendint32](../p/pq_sendint32.md): Serializes 32-bit integer (maxScale)
  - [pq_endtypsend](../p/pq_endtypsend.md): Completes serialization and returns bytea result
  - [free_var](../f/free_var.md): Cleans up temporary variable
  - PG_RETURN_BYTEA_P: Returns the serialized bytea result
- Called from (representative examples):
  - Not directly referenced by other symbols (used by aggregate framework)

## Notes and Other Information
- Only works with aggregates that don't require sumX2 (sum of squares)
- Includes error checking to prevent calls outside aggregate context
- Part of PostgreSQL's parallel aggregation support system
- Serializes all state needed to reconstruct the aggregate including special value counts
- Uses PostgreSQL's standard binary serialization protocol for cross-platform compatibility