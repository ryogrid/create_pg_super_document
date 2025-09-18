# bpcharrecv

## Location
[src/backend/utils/adt/varchar.c:230-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varchar.c#L230-L250)

## Overview
Converts external binary format to the PostgreSQL bpchar (blank-padded character) data type during binary data reception.

## Definition


## Detailed Description
The  function is a PostgreSQL I/O function that handles the conversion of binary data received from external sources (such as network protocol messages) into the internal bpchar representation. This function is part of PostgreSQL's binary protocol support, enabling efficient transmission of character data between clients and server without ASCII conversion overhead.

The function extracts the binary message text using , then delegates the actual parsing and validation to  which handles the conversion to the proper bpchar format with appropriate length constraints and padding.

## Parameters / Member Variables
-  (StringInfo): Input buffer containing the binary message data
-  (Oid): Element type OID (currently unused, marked with NOT_USED)  
-  (int32): Type modifier specifying the maximum length for the bpchar type

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgtext](../p/pq_getmsgtext.md)
  - [bpchar_input](bpchar_input.md)
  - [pfree](../p/pfree.md)
  - PG_RETURN_BPCHAR_P
- Called from (representative examples):
  - None found in current analysis

## Notes and Other Information
- This function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS
- The typelem parameter is defined but not used, indicating potential future extensibility
- Memory management is handled properly with pfree() to avoid leaks
- Part of the binary I/O protocol infrastructure for the bpchar data type