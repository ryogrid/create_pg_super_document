# oidsend

## Location
src/backend/utils/adt/oid.c: 71 - 86

## Overview
The oidsend function converts an internal OID value to binary format for transmission over PostgreSQL's wire protocol, serving as the binary output function for the OID data type.

## Definition


## Detailed Description
The oidsend function is responsible for converting PostgreSQL's internal OID representation to binary wire protocol format. It creates a StringInfo buffer, writes the OID value as a 32-bit integer in network byte order, and returns the resulting binary data. This function is part of PostgreSQL's binary I/O system and is used when OID values need to be transmitted in binary format over the network or stored in binary format for efficiency.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function call context and arguments
  -  (extracted via PG_GETARG_OID(0)): The OID value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - [pq_begintypsend](../p/pq_begintypsend.md): Initializes a StringInfo buffer for binary output
  - [pq_sendint32](../p/pq_sendint32.md): Writes a 32-bit integer to the buffer in network byte order
  - [pq_endtypsend](../p/pq_endtypsend.md): Finalizes the buffer and returns it as a bytea
  - PG_RETURN_BYTEA_P: Macro to return the binary data result
- Called from (representative examples):
  - [regprocsend](../r/regprocsend.md): Used in regproc type binary output processing
  - [regproceduresend](../r/regproceduresend.md): Used in regprocedure type binary output processing
  - [regopersend](../r/regopersend.md): Used in regoper type binary output processing
  - [regoperatorsend](../r/regoperatorsend.md): Used in regoperator type binary output processing
  - [regclasssend](../r/regclasssend.md): Used in regclass type binary output processing
  - [regcollationsend](../r/regcollationsend.md): Used in regcollation type binary output processing
  - [regtypesend](../r/regtypesend.md): Used in regtype type binary output processing
  - [regconfigsend](../r/regconfigsend.md): Used in regconfig type binary output processing
  - [regdictionarysend](../r/regdictionarysend.md): Used in regdictionary type binary output processing
  - [regrolesend](../r/regrolesend.md): Used in regrole type binary output processing
  - [regnamespacesend](../r/regnamespacesend.md): Used in regnamespace type binary output processing

## Notes and Other Information
- This function is registered as the binary send function for the OID data type in PostgreSQL's type system
- The function writes exactly 4 bytes (sizeof(uint32)) in network byte order for cross-platform compatibility
- Used extensively by the reg* family of types which are essentially OIDs with specialized output formatting
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS macro
- Part of PostgreSQL's binary protocol support for efficient data transfer
- The resulting bytea includes length information as part of the wire protocol format
- Location: src/backend/utils/adt/oid.c:71-86