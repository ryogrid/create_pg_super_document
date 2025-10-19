# brin_minmax_multi_summary_in

## Location
[src/backend/access/brin/brin_minmax_multi.c:2976-2997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L2976-L2997)

## Overview
Input function for the brin_minmax_multi_summary data type that intentionally rejects all input attempts.

## Definition

```c
Datum
brin_minmax_multi_summary_in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the input routine for the brin_minmax_multi_summary data type, which is used internally by BRIN minmax-multi indexes to represent summary data. However, since this data type is purely internal and stores data in binary format, the function is designed to reject any attempt to parse text input.

The function immediately raises an error with ERRCODE_FEATURE_NOT_SUPPORTED, preventing users from directly creating values of this type through SQL input. This is a common pattern in PostgreSQL for internal-only data types that should not be directly manipulated by users.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (ERRCODE_FEATURE_NOT_SUPPORTED)
  - [errmsg](../e/errmsg.md) (for error message formatting)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct references found (called through PostgreSQL type system)

## Notes and Other Information
- This is part of the PostgreSQL type system interface for the brin_minmax_multi_summary type
- The brin_minmax_multi_summary type is internal-only and not meant for direct user manipulation
- The corresponding output function would similarly be restricted or produce internal representations
- This pattern ensures data type safety by preventing users from creating malformed summary data
- The function includes a 'keep compiler quiet' comment for the unreachable PG_RETURN_VOID() statement
- Users cannot directly insert, update, or cast values to this type in SQL statements

## Simplified Source

```c
Datum brin_minmax_multi_summary_in(PG_FUNCTION_ARGS) {
    // Reject text input for internal-only brin_minmax_multi_summary type
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot accept a value of type %s", "brin_minmax_multi_summary")));

    PG_RETURN_VOID();  // Keep compiler quiet
}
```