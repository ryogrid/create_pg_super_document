# timetz_send

## Location
src/backend/utils/adt/date.c: 2371 - 2382

## Overview
Converts a PostgreSQL time with time zone (TIMETZ) value to its binary wire format for network transmission in the PostgreSQL protocol.

## Definition


## Detailed Description
The  function is responsible for serializing a PostgreSQL TIMETZ (time with time zone) value into binary format for transmission over the PostgreSQL wire protocol. This function is part of PostgreSQL's type input/output system and is used when data needs to be sent from the server to the client in binary format rather than text format. 

The function takes a TIMETZ value and packs its components (time and timezone offset) into a binary buffer using PostgreSQL's standard binary serialization functions. The time component is sent as a 64-bit integer and the timezone offset as a 32-bit integer.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TIMETZ argument from function args
  - pq_begintypsend: Initializes binary output buffer
  - pq_sendint64: Sends 64-bit integer (time component)
  - pq_sendint32: Sends 32-bit integer (timezone offset)
  - pq_endtypsend: Finalizes binary output buffer
  - PG_RETURN_BYTEA_P: Returns binary data as bytea
- Called from (representative examples):
  - PostgreSQL protocol handlers (indirectly through function registry)

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the binary output function for the TIMETZ type
- The binary format uses network byte order for cross-platform compatibility
- The function is called automatically by PostgreSQL when binary protocol is requested by clients
- Part of the date/time ADT (Abstract Data Type) implementation in PostgreSQL