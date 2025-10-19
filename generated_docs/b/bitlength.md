# bitlength

## Location
[src/backend/utils/adt/varbit.c:1223-1230](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1223-L1230)

## Overview
Implements the SQL BIT_LENGTH() function that returns the length of a bit string in bits.

## Definition
```c
Datum bitlength(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bitlength` function is a PostgreSQL built-in function that implements the SQL standard BIT_LENGTH() operation for bit strings. It returns the total number of bits in the input bit string as a 32-bit integer. This function provides a straightforward way to determine the size of bit string data.

The function is implemented as a simple wrapper around the `VARBITLEN` macro, which efficiently extracts the bit length information from the VarBit structure's header. This makes the operation very fast since the length is stored as metadata rather than requiring traversal of the bit data.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input bit string (VarBit*) - the bit string whose length is to be determined
- `arg`: Local variable - extracted VarBit pointer from function arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (argument extraction macro)
  - PG_RETURN_INT32 (return value macro for 32-bit integer)
  - VARBITLEN (macro to extract bit length from VarBit structure)
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/varbit.c:1223-1230
- This is a PostgreSQL built-in function accessible via SQL as BIT_LENGTH()
- Returns the exact number of bits, not bytes or characters
- Very efficient implementation using pre-stored length metadata
- Companion to bitoctetlength() which returns the length in bytes
- Part of the SQL standard bit string functions
- Returns int32, which can handle bit strings up to ~2.1 billion bits

## Simplified Source

```c
Datum bitlength(PG_FUNCTION_ARGS) {
    // Extract the bit string argument
    VarBit *arg = PG_GETARG_VARBIT_P(0);

    // Return the length in bits (stored in header)
    PG_RETURN_INT32(VARBITLEN(arg));
}
```