# xid8out

## Location
[src/backend/utils/adt/xid.c:192-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L192-L201)

## Overview
Converts a FullTransactionId (XID8) to its string representation, formatting it as a decimal number for display or serialization.

## Definition

```c
Datum
xid8out(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is PostgreSQL's output conversion function for the XID8 data type. It takes a FullTransactionId and converts it to a human-readable string representation. This function is automatically called by PostgreSQL's type system when displaying XID8 values or converting them to text format.

The function allocates a 21-character buffer (sufficient for the maximum uint64 value plus null terminator) and uses  with the  macro to format the 64-bit transaction ID as a decimal string. The conversion process extracts the underlying uint64 value from the FullTransactionId using .

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS): A FullTransactionId (XID8) value to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts FullTransactionId from function arguments
  -  - PostgreSQL memory allocation function
  -  - Standard C string formatting function
  -  - Extracts uint64 from FullTransactionId
  -  - Returns C-string as PostgreSQL Datum
- Macros used:
  -  - Platform-appropriate printf format string for uint64
- Types referenced:
  -  - 64-bit transaction identifier type
- Called from:
  - No direct callers found (invoked automatically by PostgreSQL's type system during output conversion)

## Notes and Other Information
- Buffer size of 21 characters accommodates the maximum 64-bit unsigned integer value (18446744073709551615) plus null terminator
- The function uses PostgreSQL's memory management through  rather than standard malloc
- Output format is always decimal, providing a consistent string representation across platforms
- Located in src/backend/utils/adt/xid.c alongside other transaction ID utility functions

## Simplified Source

```c
Datum xid8out(PG_FUNCTION_ARGS) {
    // Get the 64-bit full transaction ID from function arguments
    FullTransactionId fxid = PG_GETARG_FULLTRANSACTIONID(0);

    // Allocate buffer for string representation (21 chars for max uint64 + null)
    char *result = (char *) palloc(21);

    // Format the transaction ID as a decimal string
    snprintf(result, 21, UINT64_FORMAT, U64FromFullTransactionId(fxid));

    // Return the formatted string
    PG_RETURN_CSTRING(result);
}
```