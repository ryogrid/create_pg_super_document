# cash_recv

## Location
[src/backend/utils/adt/cash.c:590-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L590-L600)

## Overview
A PostgreSQL binary input function that converts external binary format data to a Cash data type value.

## Definition
```c
Datum cash_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary I/O system for the Cash data type. It reads a 64-bit integer from a binary input buffer and converts it directly to a Cash value. The function is used when PostgreSQL needs to deserialize Cash values from binary protocol messages, such as during network communication with clients using the binary protocol or when reading from binary-format files. The implementation is straightforward since Cash is internally represented as a 64-bit integer, so no complex conversion logic is needed.

## Parameters / Member Variables
- Takes a StringInfo buffer through PostgreSQL's function argument system (PG_GETARG_POINTER)
- Internal variables:
  - `buf`: StringInfo buffer containing the binary data to be read

## Dependencies
- Functions called/Symbols referenced:
  - Cash (data type)
  - StringInfo (buffer type)
  - [pq_getmsgint64](../p/pq_getmsgint64.md) (function to read 64-bit integer from binary message)
  - PG_RETURN_CASH (return macro for Cash values)
- Called from:
  - This appears to be a top-level receive function, likely called by PostgreSQL's type system during binary deserialization

## Notes and Other Information
- This is a PostgreSQL-style binary input function following the fmgr (function manager) calling convention
- Part of the binary I/O protocol support for the Cash data type
- Complementary function to cash_send (which would serialize Cash to binary format)
- Used in binary protocol communication between PostgreSQL server and clients
- The implementation is simple because Cash has a direct mapping to a 64-bit integer representation
- No validation or error checking is performed since the binary format is assumed to be trusted
- Essential for efficient binary data transfer of monetary values in PostgreSQL