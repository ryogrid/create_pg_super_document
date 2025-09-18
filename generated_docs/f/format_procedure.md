# format_procedure

## Location
[src/backend/utils/adt/regproc.c:299-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L299-L304)

## Overview
Converts a procedure OID to its string representation in "procedure_name(arguments)" format for use by backend modules.

## Definition
```c
char *format_procedure(Oid procedure_oid)
```

## Detailed Description
The `format_procedure` function provides a simple interface for converting procedure OIDs to their human-readable string representation. It serves as a convenience wrapper around `format_procedure_extended` with default formatting options (flags = 0).

This function is primarily used throughout the PostgreSQL backend when procedure names need to be displayed in error messages, validation output, logging, or other informational contexts. The returned string includes both the procedure name and its argument type list in a format that can be parsed back by the corresponding input functions.

The function allocates memory for the result string using PostgreSQL's memory management (palloc), so the caller is responsible for managing the memory appropriately within the current memory context.

## Parameters / Member Variables
- `procedure_oid`: The OID of the procedure to format

## Dependencies
- Functions called/Symbols referenced:
  - [format_procedure_extended](format_procedure_extended.md): Core formatting function with extended options
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md): BRIN index validation error messages
  - [ginvalidate](../g/ginvalidate.md): GIN index validation error messages  
  - [gistvalidate](../g/gistvalidate.md): GiST index validation error messages
  - [hashvalidate](../h/hashvalidate.md): Hash index validation error messages
  - [btvalidate](../b/btvalidate.md): B-tree index validation error messages
  - [spgvalidate](../s/spgvalidate.md): SP-GiST index validation error messages
  - [getObjectDescription](../g/getObjectDescription.md): Object description formatting
  - [ProcedureCreate](../P/ProcedureCreate.md): Procedure creation validation and errors
  - [regprocedureout](../r/regprocedureout.md): Output function for regprocedure type

## Notes and Other Information
- Returns a palloc'd string that must be managed within PostgreSQL's memory context system
- Used extensively in validation functions across different index access methods
- Provides consistent procedure name formatting throughout the backend
- The default formatting (flags = 0) uses standard qualification rules
- Part of PostgreSQL's object formatting infrastructure for error reporting and logging
- Widely used in error messages where procedure identification is needed