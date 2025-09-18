# cidin

## Location
src/backend/utils/adt/xid.c: 322 - 334

## Overview
Converts a string representation of a command identifier to its internal CommandId representation, serving as the input conversion function for PostgreSQL's cid data type.

## Definition
```c
Datum cidin(PG_FUNCTION_ARGS)
```

## Detailed Description
The cidin function is part of PostgreSQL's command identifier (cid) data type implementation. It takes a C-string representation of a command identifier and converts it to the internal CommandId format. The function uses the uint32in_subr utility function to perform the actual string-to-integer conversion, ensuring proper error handling and validation. Command identifiers are used internally by PostgreSQL to track the order of commands within a single transaction, which is essential for implementing proper MVCC (Multi-Version Concurrency Control) semantics.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: C-string (char*) representation of the command identifier to convert

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type definition for command identifiers)
  - [uint32in_subr](../u/uint32in_subr.md) (utility function for string-to-uint32 conversion with error handling)
  - PG_RETURN_COMMANDID (macro for returning CommandId values)
- Called from (representative examples):
  - SQL input operations for cid data type
  - COPY operations involving cid columns
  - Internal PostgreSQL data conversion routines

## Notes and Other Information
- Part of PostgreSQL's Command Identifier subsystem for MVCC support
- Handles input validation and error reporting through uint32in_subr
- [Command](../C/Command.md) identifiers are crucial for tracking command order within transactions
- The "cid" parameter in uint32in_subr provides context for error messages
- Located in src/backend/utils/adt/xid.c:322-334
- Complemented by cidout function for output conversion