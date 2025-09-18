# regtyperecv

## Location
[src/backend/utils/adt/regproc.c:1295-1304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1295-L1304)

## Overview
Converts external binary format data to regtype, serving as the binary input function for the regtype data type.

## Definition


## Detailed Description
The  function is PostgreSQL's binary receive function for the regtype data type. It is responsible for converting data from PostgreSQL's external binary format into an internal regtype value. The function is a simple wrapper that delegates all processing to the  function, since regtype is internally represented as an OID.

This function is part of PostgreSQL's type system infrastructure and is called automatically when binary data needs to be converted to regtype format, such as during network communication or binary data transfer operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro
  - Contains binary format data to be converted to regtype
  - Includes additional context like format information and buffer details

## Dependencies
- Functions called/Symbols referenced:
  - : OID binary receive function that handles the actual conversion
- Called from (representative examples):
  - No direct references found in the codebase (called by PostgreSQL's type system infrastructure)

## Notes and Other Information
- Simple delegation to oidrecv since regtype is internally an OID
- Part of PostgreSQL's binary I/O system for the regtype data type
- Companion to regtypesend for binary serialization/deserialization
- Located in src/backend/utils/adt/regproc.c
- Handles binary protocol conversion automatically within PostgreSQL's type system