# numeric_avg_deserialize

## Location
[src/backend/utils/adt/numeric.c:5272-5329](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5272-L5329)

## Overview
Deserializes a bytea back into NumericAggState for numeric aggregates that don't require sumX2, reconstructing the aggregate state from binary format.

## Definition
```c
Datum numeric_avg_deserialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs the inverse operation of numeric_avg_serialize, converting a serialized bytea back into a NumericAggState structure. It's a critical component of PostgreSQL's parallel aggregation system, allowing aggregate states to be reconstructed after being transmitted between processes or stored.

The function reads the binary data in the exact same order it was written during serialization: count of values (N), sum of values (sumX), scale information, and counts for special numeric values. It uses PostgreSQL's standard message receiving functions to ensure proper endianness handling and data integrity across different platforms.

The deserialized state is allocated in the current memory context and can be used to continue aggregate computation or combine with other partial aggregate states.

## Parameters / Member Variables
- `fcinfo`: Function call information containing the bytea as argument 0

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md): Validates aggregate context
  - PG_GETARG_BYTEA_PP: Retrieves the serialized bytea argument
  - init_var: Initializes temporary NumericVar
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md): Sets up buffer for reading binary data
  - [makeNumericAggStateCurrentContext](../m/makeNumericAggStateCurrentContext.md): Creates new NumericAggState in current context
  - [pq_getmsgint64](../p/pq_getmsgint64.md): Deserializes 64-bit integers (N, maxScaleCount, NaNcount, pInfcount, nInfcount)
  - [numericvar_deserialize](numericvar_deserialize.md): Deserializes the numeric sum value
  - [accum_sum_add](../a/accum_sum_add.md): Adds deserialized sum to the aggregate state
  - [pq_getmsgint](../p/pq_getmsgint.md): Deserializes 32-bit integer (maxScale)
  - [pq_getmsgend](../p/pq_getmsgend.md): Validates end of message buffer
  - [free_var](../f/free_var.md): Cleans up temporary variable
- Called from (representative examples):
  - Not directly referenced by other symbols (used by aggregate framework)

## Notes and Other Information
- Companion function to numeric_avg_serialize for parallel aggregation
- Only works with aggregates that don't require sumX2 (sum of squares)
- Includes comprehensive error checking for aggregate context and message format
- Part of PostgreSQL's parallel processing infrastructure
- Uses read-only StringInfo for efficient binary data parsing
- Allocates result in current memory context for proper lifecycle management

## Simplified Source

```c
Datum numeric_avg_deserialize(PG_FUNCTION_ARGS) {
    bytea *sstate;
    NumericAggState *result;
    StringInfoData buf;
    NumericVar tmp_var;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, NULL))
        elog(ERROR, "aggregate function called in non-aggregate context");

    sstate = PG_GETARG_BYTEA_PP(0);
    init_var(&tmp_var);

    // Setup buffer for reading binary data
    initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

    // Create new aggregate state
    result = makeNumericAggStateCurrentContext(false);

    // Deserialize all state components in same order as serialization
    result->N = pq_getmsgint64(&buf);                // Count of values

    numericvar_deserialize(&buf, &tmp_var);          // Sum value
    accum_sum_add(&(result->sumX), &tmp_var);

    result->maxScale = pq_getmsgint(&buf, 4);        // Scale info
    result->maxScaleCount = pq_getmsgint64(&buf);
    result->NaNcount = pq_getmsgint64(&buf);         // Special value counts
    result->pInfcount = pq_getmsgint64(&buf);
    result->nInfcount = pq_getmsgint64(&buf);

    // Validate message end and cleanup
    pq_getmsgend(&buf);
    free_var(&tmp_var);

    PG_RETURN_POINTER(result);
}
```