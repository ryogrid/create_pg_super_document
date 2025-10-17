# regnamespacerecv

## Location
[src/backend/utils/adt/regproc.c:1750-1759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1750-L1759)

## Overview
Converts external binary format data to the regnamespace PostgreSQL data type, which represents namespace (schema) object identifiers.

## Definition

```c
Datum
regnamespacerecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a binary input function for the regnamespace data type in PostgreSQL. It handles the conversion of binary format data (as received from external sources like client applications) into PostgreSQL's internal regnamespace representation. This function is part of PostgreSQL's type system infrastructure and is used internally when binary protocol communication occurs.

The function implementation is straightforward - it delegates all functionality to , indicating that regnamespace values are internally handled identically to OID values in their binary representation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [oidrecv](../o/oidrecv.md)
- Called from (representative examples):
  - Used internally by PostgreSQL's type system for binary protocol handling
  - Not directly referenced by other user-visible functions

## Notes and Other Information
- This function shares its implementation with , reflecting the fact that regnamespace is essentially an OID with semantic meaning (namespace identifier)
- Part of the regproc family of functions that handle various registry data types
- Located in src/backend/utils/adt/regproc.c alongside other registry type functions
- Binary input functions like this are typically called during binary protocol communication between PostgreSQL server and clients

## Simplified Source

```c
Datum regnamespacerecv(PG_FUNCTION_ARGS) {
    // Delegates to oidrecv since regnamespace is internally an OID
    return oidrecv(fcinfo);
}
```