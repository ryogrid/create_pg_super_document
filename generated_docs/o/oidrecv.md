# oidrecv

## Location
src/backend/utils/adt/oid.c: 60 - 70

## Overview
The oidrecv function converts external binary format data to an internal OID value, serving as the binary input function for the OID data type in PostgreSQL's wire protocol.

## Definition


## Detailed Description
The oidrecv function is responsible for reading OID values from PostgreSQL's binary wire protocol format. It extracts a binary integer from a StringInfo buffer using pq_getmsgint and converts it to PostgreSQL's internal OID representation. This function is part of PostgreSQL's binary I/O system and is used when OID values are transmitted in binary format over the network or stored in binary format.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
  -  (extracted via PG_GETARG_POINTER(0)): StringInfo buffer containing the binary representation of the OID

## Dependencies
- Functions called/Symbols referenced:
  - pq_getmsgint: Extracts a binary integer from the message buffer with specified byte size
  - PG_RETURN_OID: Macro to return the converted OID value
- Called from (representative examples):
  - regprocrecv: Used in regproc type binary input processing
  - regprocedurerecv: Used in regprocedure type binary input processing
  - regoperrecv: Used in regoper type binary input processing
  - regoperatorrecv: Used in regoperator type binary input processing
  - regclassrecv: Used in regclass type binary input processing
  - regcollationrecv: Used in regcollation type binary input processing
  - regtyperecv: Used in regtype type binary input processing
  - regconfigrecv: Used in regconfig type binary input processing
  - regdictionaryrecv: Used in regdictionary type binary input processing
  - regrolerecv: Used in regrole type binary input processing
  - regnamespacerecv: Used in regnamespace type binary input processing

## Notes and Other Information
- This function is registered as the binary receive function for the OID data type in PostgreSQL's type system
- The function reads exactly sizeof(Oid) bytes from the buffer, ensuring proper alignment and endianness handling
- Used extensively by the reg* family of types which are essentially OIDs with specialized output formatting
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS macro
- Part of PostgreSQL's binary protocol support for efficient data transfer
- Location: src/backend/utils/adt/oid.c:60-70