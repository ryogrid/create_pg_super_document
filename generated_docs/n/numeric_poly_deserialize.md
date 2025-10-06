# numeric_poly_deserialize

## Location
[src/backend/utils/adt/numeric.c:5755-5807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5755-L5807)

## Overview
The numeric_poly_deserialize function deserializes a bytea representation back into a PolyNumAggState structure for PostgreSQL's parallel aggregation framework, reconstructing partial aggregate states from serialized data.

## Definition
```c
Datum numeric_poly_deserialize(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the counterpart to numeric_poly_serialize, responsible for reconstructing a PolyNumAggState structure from its serialized bytea representation. It's a critical component of PostgreSQL's parallel aggregation infrastructure, enabling the reconstruction of partial aggregate states that have been transmitted between processes or stored temporarily.

The function reads the serialized data using PostgreSQL's binary protocol format, extracting the count (N), sum of values (sumX), and sum of squares (sumX2). It handles platform differences by converting the standardized numeric format back to the appropriate internal representation (128-bit integers when available, or accumulator sums otherwise). This ensures compatibility across different architectures while maintaining optimal performance on each platform.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - Argument 0: bytea pointer (the serialized state to deserialize)

## Dependencies
- Functions called/Symbols referenced:
  - PolyNumAggState (data structure)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (context validation)
  - [StringInfoData](../S/StringInfoData.md) (buffer for deserialization)
  - [NumericVar](../N/NumericVar.md) (temporary numeric variable)
  - PG_GETARG_BYTEA_PP (extract bytea argument)
  - init_var (numeric variable initialization)
  - [initReadOnlyStringInfo](../i/initReadOnlyStringInfo.md) (initialize read buffer)
  - makePolyNumAggStateCurrentContext (create new state structure)
  - [pq_getmsgint64](../p/pq_getmsgint64.md) (deserialize int64 value)
  - [numericvar_deserialize](numericvar_deserialize.md) (deserialize numeric variable)
  - [numericvar_to_int128](numericvar_to_int128.md) (convert numeric to 128-bit int, when HAVE_INT128)
  - [accum_sum_add](../a/accum_sum_add.md) (add to accumulator sum)
  - [pq_getmsgend](../p/pq_getmsgend.md) (finalize message reading)
  - [free_var](../f/free_var.md) (cleanup numeric variable)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is the complement to numeric_poly_serialize, completing the serialization/deserialization cycle
- Handles platform differences transparently by converting from standardized numeric format to optimal internal representation
- Part of PostgreSQL's parallel aggregation system for statistical functions requiring sum-of-squares calculations
- Uses PostgreSQL's binary protocol format for efficient deserialization
- The function validates that it's called in an appropriate aggregate context
- Memory management includes proper cleanup of temporary variables and creation of result in appropriate context
- Critical for distributed query processing where partial results are received from other processes or nodes
- Maintains consistency and accuracy of statistical calculations across parallel operations

## Simplified Source

```c
Datum numeric_poly_deserialize(PG_FUNCTION_ARGS) {
    bytea *sstate;
    PolyNumAggState *result;
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
    result = makePolyNumAggStateCurrentContext(false);

    // Deserialize count
    result->N = pq_getmsgint64(&buf);

    // Deserialize and convert sumX (platform-appropriate format)
    numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
    numericvar_to_int128(&tmp_var, &result->sumX);
#else
    accum_sum_add(&result->sumX, &tmp_var);
#endif

    // Deserialize and convert sumX2 (platform-appropriate format)
    numericvar_deserialize(&buf, &tmp_var);
#ifdef HAVE_INT128
    numericvar_to_int128(&tmp_var, &result->sumX2);
#else
    accum_sum_add(&result->sumX2, &tmp_var);
#endif

    // Validate message end and cleanup
    pq_getmsgend(&buf);
    free_var(&tmp_var);

    PG_RETURN_POINTER(result);
}
```