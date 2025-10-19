# varbit

## Location
[src/backend/utils/adt/varbit.c:742-773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L742-L773)

## Overview
Performs length coercion of variable-length bit strings to fit specified maximum length constraints, with different behavior for explicit vs implicit casts.

## Definition
```c
Datum varbit(PG_FUNCTION_ARGS)
```

## Detailed Description
The `varbit` function implements length coercion for PostgreSQL's variable-length bit string type. It ensures that bit strings conform to the maximum length specified in column definitions or explicit casts. The function handles two distinct casting modes: implicit casts (which raise errors on overflow) and explicit casts (which silently truncate excess data).

When the target length is invalid (≤ 0) or the source data already fits within the constraint, the function returns the original data unchanged for efficiency. For implicit casts that would require truncation, the function raises a STRING_DATA_RIGHT_TRUNCATION error. For explicit casts, it performs silent truncation by creating a new VarBit structure with the specified length and copying the appropriate number of bytes.

The function ensures data integrity by properly zero-padding the final byte after truncation using the VARBIT_PAD macro, maintaining the invariant that unused bits in the last byte are always zero.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg`: Input VarBit pointer via `PG_GETARG_VARBIT_P(0)`
  - `len`: Maximum bit length constraint via `PG_GETARG_INT32(1)`
  - `isExplicit`: Boolean indicating explicit vs implicit cast via `PG_GETARG_BOOL(2)`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P
  - PG_GETARG_INT32
  - PG_GETARG_BOOL
  - VARBITLEN
  - VARBITTOTALLEN
  - SET_VARSIZE
  - VARBITS
  - VARBITBYTES
  - VARBIT_PAD
  - PG_RETURN_VARBIT_P
  - memcpy
- Called from:
  - No direct callers found (called by PostgreSQL's casting system)

## Notes and Other Information
- Implements PostgreSQL's standard casting semantics: implicit casts error on overflow, explicit casts truncate
- Optimizes the common case where no truncation is needed by returning the original data
- Uses memcpy for efficient byte-level copying during truncation
- Maintains data integrity through proper zero-padding of the final byte
- Supports both column constraint enforcement and explicit user casts
- Part of PostgreSQL's type system for VARBIT data type operations
- The len parameter represents the maximum number of bits, not bytes
- Located in src/backend/utils/adt/varbit.c:742-773

## Simplified Source

```c
Datum varbit(PG_FUNCTION_ARGS) {
    VarBit *arg = PG_GETARG_VARBIT_P(0);
    int32 len = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);

    // No work needed if length is invalid or data already fits
    if (len <= 0 || len >= VARBITLEN(arg))
        PG_RETURN_VARBIT_P(arg);

    // For implicit casts, error if truncation would occur
    if (!isExplicit)
        ereport(ERROR,
                (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                 errmsg("bit string too long for type bit varying(%d)", len)));

    // Create truncated result for explicit casts
    int rlen = VARBITTOTALLEN(len);
    VarBit *result = palloc(rlen);
    SET_VARSIZE(result, rlen);
    VARBITLEN(result) = len;

    // Copy data and ensure proper padding
    memcpy(VARBITS(result), VARBITS(arg), VARBITBYTES(result));
    VARBIT_PAD(result);

    PG_RETURN_VARBIT_P(result);
}
```