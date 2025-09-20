# regrolerecv

## Location
[src/backend/utils/adt/regproc.c:1633-1642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1633-L1642)

## Overview
The regrolerecv function converts external binary format data to the regrole data type, which is used to store references to database roles (users/groups) by their OID.

## Definition

```c
Datum
regrolerecv(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL input/output function that handles the conversion of binary format data to the regrole data type. The regrole type is a special object identifier (OID) type that specifically references database roles. The function is implemented as a simple wrapper that delegates all processing to the oidrecv function, since regrole values are internally stored as OIDs and the binary representation is identical.

The function follows PostgreSQL's standard function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum type. It is part of PostgreSQL's type system infrastructure that enables proper serialization and deserialization of regrole values in binary format, which is used for efficient data transfer in the PostgreSQL wire protocol and for binary I/O operations.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS) which provides access to:
  - Function call context information
  - Input arguments in binary format
  - Memory context for result allocation

## Dependencies
- Functions called/Symbols referenced:
  - [oidrecv](../o/oidrecv.md) (delegates all processing to this function)
- Called from (representative examples):
  - This function is typically invoked by PostgreSQL's type system when binary input conversion is needed for regrole values

## Notes and Other Information
- The function is implemented as a direct delegation to oidrecv since regrole values are internally represented as OIDs
- This is part of PostgreSQL's reg* family of types (regproc, regtype, regrole, etc.) that provide human-readable representations of system catalog objects
- The binary format for regrole is identical to that of OID, which explains the code reuse
- Located in src/backend/utils/adt/regproc.c alongside other reg* type functions