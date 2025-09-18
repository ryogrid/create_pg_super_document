# xid8recv

## Location
[src/backend/utils/adt/xid.c:202-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L202-L211)

## Overview
Receives a FullTransactionId (XID8) value from a binary message buffer during PostgreSQL's binary protocol communication.

## Definition


## Detailed Description
The  function is PostgreSQL's binary input function for the XID8 data type. It deserializes a FullTransactionId from a binary message buffer, typically used during client-server communication when binary protocol is employed or during internal data transfer operations. This function is part of PostgreSQL's type system infrastructure for handling binary format data.

The function extracts a StringInfo buffer from the function arguments, reads a 64-bit integer from the buffer using , and converts the resulting value to a FullTransactionId. This process is the binary counterpart to the text-based  function.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS): A StringInfo buffer containing binary-encoded transaction ID data

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts pointer from function arguments
  -  - Reads 64-bit integer from message buffer
  -  - Converts uint64 to FullTransactionId
  -  - Returns FullTransactionId as PostgreSQL Datum
- Types referenced:
  -  - PostgreSQL message buffer type
- Called from:
  - No direct callers found (invoked automatically by PostgreSQL's type system during binary deserialization)

## Notes and Other Information
- This function is essential for PostgreSQL's binary protocol support, enabling efficient transmission of XID8 values
- Complements the  function to provide complete binary serialization/deserialization support
- Uses PostgreSQL's message buffer infrastructure for safe binary data handling
- The function assumes the buffer contains properly formatted binary data as written by 
- Located in src/backend/utils/adt/xid.c alongside other transaction ID utility functions