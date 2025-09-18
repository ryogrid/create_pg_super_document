# int8recv

## Location
[src/backend/utils/adt/int8.c:83-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L83-L93)

## Overview
Converts external binary format data to PostgreSQL's internal int8 (bigint) representation.

## Definition


## Detailed Description
The  function serves as the binary input conversion routine for PostgreSQL's int8 data type (bigint). It takes binary data from the PostgreSQL wire protocol format and converts it to the internal Datum representation. This function is part of the PostgreSQL type system's binary input/output infrastructure and is used when receiving int8 values in binary format during client-server communication, particularly in prepared statements and binary result sets.

The function reads a 64-bit integer directly from the binary message buffer using the protocol-specific  function, which handles proper byte order conversion.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  -  (StringInfo): Binary message buffer containing the 64-bit integer in network byte order

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract pointer argument (StringInfo buffer)
  - : Protocol function to read 64-bit integer from binary message buffer
  - : Macro to return int64 value as Datum
- Called from (representative examples):
  - No direct references found in the current codebase (used internally by the type system)

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the binary receive function for the int8/bigint data type
- Used primarily in binary protocol communication between client and server
- The StringInfo buffer parameter contains data in PostgreSQL's wire protocol format
- Handles network byte order conversion automatically through 
- Part of the binary I/O functions that enable efficient data transfer without string conversion overhead
- Located in src/backend/utils/adt/int8.c alongside other 64-bit integer utility functions