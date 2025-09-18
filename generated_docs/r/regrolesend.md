# regrolesend

## Location
src/backend/utils/adt/regproc.c: 1643 - 1657

## Overview
The regrolesend function converts regrole data type values to external binary format for efficient data transfer and storage in PostgreSQL.

## Definition


## Detailed Description
This function is a PostgreSQL output function that handles the conversion of regrole data type values to binary format. The regrole type is a special object identifier (OID) type that specifically references database roles (users/groups). The function is implemented as a simple wrapper that delegates all processing to the oidsend function, since regrole values are internally stored as OIDs and their binary representation is identical to regular OID values.

The function follows PostgreSQL's standard function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum type. It is part of PostgreSQL's type system infrastructure that enables proper serialization of regrole values for binary I/O operations, which is used in the PostgreSQL wire protocol for efficient client-server communication and for binary storage operations.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS) which provides access to:
  - Function call context information
  - Input regrole value to be converted
  - Memory context for result allocation

## Dependencies
- Functions called/Symbols referenced:
  - oidsend (delegates all processing to this function)
- Called from (representative examples):
  - This function is typically invoked by PostgreSQL's type system when binary output conversion is needed for regrole values

## Notes and Other Information
- The function is implemented as a direct delegation to oidsend since regrole values are internally represented as OIDs
- This is the binary output counterpart to regrolerecv, together forming the binary I/O pair for the regrole type
- The binary format produced is identical to that of OID values, which explains the code reuse with oidsend
- Part of PostgreSQL's reg* family of types that provide human-readable representations of system catalog objects
- Located in src/backend/utils/adt/regproc.c alongside other reg* type functions