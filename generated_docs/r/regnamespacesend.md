# regnamespacesend

## Location
src/backend/utils/adt/regproc.c: 1760 - 1773

## Overview
Converts regnamespace PostgreSQL data type to external binary format for transmission to client applications.

## Definition
```c
Datum regnamespacesend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `regnamespacesend` function is a binary output function for the regnamespace data type in PostgreSQL. It handles the conversion of PostgreSQL's internal regnamespace representation into binary format suitable for transmission to external clients. This function is part of PostgreSQL's type system infrastructure and is used internally when binary protocol communication occurs.

Like its counterpart `regnamespacerecv`, this function implementation delegates all functionality to `oidsend`, confirming that regnamespace values are internally handled identically to OID values in their binary representation.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro - standard PostgreSQL function calling convention that provides access to function arguments and context through the `fcinfo` parameter

## Dependencies
- Functions called/Symbols referenced:
  - [oidsend](../o/oidsend.md)
- Called from (representative examples):
  - Used internally by PostgreSQL's type system for binary protocol handling
  - Not directly referenced by other user-visible functions

## Notes and Other Information
- This function shares its implementation with `oidsend`, reflecting the fact that regnamespace is essentially an OID with semantic meaning (namespace identifier)
- Counterpart to `regnamespacerecv` - handles the output direction of binary format conversion
- Part of the regproc family of functions that handle various registry data types
- Located in src/backend/utils/adt/regproc.c alongside other registry type functions
- Binary output functions like this are typically called during binary protocol communication between PostgreSQL server and clients