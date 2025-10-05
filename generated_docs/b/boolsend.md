# boolsend

## Location
[src/backend/utils/adt/bool.c:187-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L187-L203)

## Overview
Converts a PostgreSQL boolean value to binary format for network transmission or storage.

## Definition

```c
Datum
boolsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is part of PostgreSQL's type system that handles the binary serialization of boolean values. It takes a boolean input parameter and converts it to a binary representation suitable for network transmission or binary storage. The function follows PostgreSQL's standard binary output protocol, creating a binary buffer and writing a single byte (1 for true, 0 for false) to represent the boolean value.

This function is typically used internally by PostgreSQL when sending boolean data in binary format to client applications or when storing boolean values in binary format.

## Parameters / Member Variables
- Input parameter (accessed via ): The boolean value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves boolean argument from function call
  -  - Initializes binary output buffer
  -  - Writes a single byte to the buffer
  -  - Finalizes binary output buffer
  -  - Returns the binary data as a bytea type

- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- The function converts true to byte value 1 and false to byte value 0
- This is the binary output function counterpart to  (binary input function)
- Part of PostgreSQL's type system infrastructure for boolean data type
- Located in  at lines 187-203

## Simplified Source

```c
Datum
boolsend(PG_FUNCTION_ARGS)
{
    bool arg1 = PG_GETARG_BOOL(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Send 1 for true, 0 for false
    pq_sendbyte(&buf, arg1 ? 1 : 0);

    // Return the binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```