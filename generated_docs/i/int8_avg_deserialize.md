# int8_avg_deserialize

## Location
[src/backend/utils/adt/numeric.c:5944-5989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5944-L5989)

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

## Simplified Source

```c
Datum
int8_avg_deserialize(PG_FUNCTION_ARGS)
{
    bytea *sstate;
    PolyNumAggState *result;
    StringInfoData buf;
    NumericVar tmp_var;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, NULL))
        elog(ERROR, "aggregate function called in non-aggregate context");

    sstate = PG_GETARG_BYTEA_PP(0);

    // Initialize for deserialization
    init_var(&tmp_var);
    initReadOnlyStringInfo(&buf, VARDATA_ANY(sstate), VARSIZE_ANY_EXHDR(sstate));

    // Create new aggregate state
    result = makePolyNumAggStateCurrentContext(false);

    // Deserialize count
    result->N = pq_getmsgint64(&buf);

    // Deserialize sum - convert from standard numeric format
    numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
    numericvar_to_int128(&tmp_var, &result->sumX);
#else
    accum_sum_add(&result->sumX, &tmp_var);
#endif

    // Complete deserialization
    pq_getmsgend(&buf);
    free_var(&tmp_var);

    return result;
}
```