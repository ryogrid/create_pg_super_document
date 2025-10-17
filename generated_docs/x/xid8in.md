# xid8in

## Location
[src/backend/utils/adt/xid.c:182-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L182-L191)

## Overview
Parses a string representation of a transaction ID and converts it to a FullTransactionId (XID8) data type.

## Definition

```c
Datum
xid8in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is PostgreSQL's input conversion function for the XID8 data type. It takes a string representation of a transaction ID and converts it into the internal FullTransactionId representation. This function is part of PostgreSQL's type system infrastructure and is automatically called when converting string literals or text values to the XID8 type.

The function uses  to parse the input string as a 64-bit unsigned integer, then converts the resulting value to a FullTransactionId using . Error handling and validation are performed by the underlying  function.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS): A C-string containing the text representation of a transaction ID

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts C-string from function arguments
  -  - Parses string as 64-bit unsigned integer with error handling
  -  - Converts uint64 to FullTransactionId
  -  - Returns FullTransactionId as PostgreSQL Datum
- Called from:
  - No direct callers found (invoked automatically by PostgreSQL's type system during input conversion)

## Notes and Other Information
- This is an essential function for PostgreSQL's type system, enabling conversion from textual representations to XID8 values
- The function leverages existing uint64 parsing infrastructure to handle string conversion
- Error messages and validation are handled by uint64in_subr with the type name 'xid8' for context
- Located in src/backend/utils/adt/xid.c alongside other transaction ID utility functions

## Simplified Source

```c
Datum xid8in(PG_FUNCTION_ARGS) {
    char *input_string = PG_GETARG_CSTRING(0);
    uint64 parsed_value;

    // Parse string as 64-bit unsigned integer
    parsed_value = uint64in_subr(input_string, NULL, "xid8", fcinfo->context);

    // Convert to FullTransactionId and return
    PG_RETURN_FULLTRANSACTIONID(FullTransactionIdFromU64(parsed_value));
}
```