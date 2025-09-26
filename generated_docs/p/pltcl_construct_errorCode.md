# pltcl_construct_errorCode

## Location
[src/pl/tcl/pltcl.c:1846-1990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L1846-L1990)

## Overview
Constructs a comprehensive Tcl errorCode list containing detailed PostgreSQL error information for proper error handling in PL/Tcl procedures.

## Definition
```c
static void
pltcl_construct_errorCode(Tcl_Interp *interp, ErrorData *edata)
```

## Detailed Description
This function creates a structured Tcl errorCode list that provides comprehensive error information from PostgreSQL's ErrorData structure to Tcl procedures. The errorCode list follows Tcl conventions for structured error reporting and includes both mandatory fields (POSTGRES version, SQLSTATE, condition name, message) and optional context-specific information when available.

The function systematically processes all available error information from the PostgreSQL ErrorData structure, converting database-encoded strings to UTF-8 for proper Tcl handling. It constructs a flat list of key-value pairs that can be easily processed by Tcl error handling code, providing detailed diagnostic information including schema/table/column names, constraint information, source code locations, and internal query details.

The errorCode structure enables PL/Tcl procedures to implement sophisticated error handling by examining specific error conditions, extracting relevant context information, and making informed decisions about error recovery or propagation.

## Parameters / Member Variables
- `interp`: Tcl interpreter where the errorCode will be set
- `edata`: PostgreSQL ErrorData structure containing comprehensive error information

## Dependencies
- Functions called/Symbols referenced:
  - Tcl_NewObj
  - Tcl_ListObjAppendElement
  - Tcl_NewStringObj
  - Tcl_NewIntObj
  - Tcl_SetObjErrorCode
  - unpack_sql_state
  - pltcl_get_condition_name
  - UTF_E2U
- Called from (representative examples):
  - pltcl_elog
  - pltcl_subtrans_abort
  - pltcl_commit
  - pltcl_rollback

## Notes and Other Information
- Creates errorCode as a flat list with alternating keys and values for easy Tcl processing
- Always includes mandatory fields: "POSTGRES", version, "SQLSTATE", state code, "condition", condition name, "message", error message
- Conditionally includes optional fields only when present in ErrorData: detail, hint, context, schema, table, column, datatype, constraint, statement, cursor_position, filename, lineno, funcname
- Uses UTF_E2U conversion for all string values to ensure proper UTF-8 encoding for Tcl
- Handles both string and integer values appropriately (cursor_position and lineno as integers)
- Excludes cursorpos field in favor of internalpos for more relevant error positioning information
- Provides complete error context for sophisticated error handling and debugging in PL/Tcl procedures
- Sets the errorCode using Tcl_SetObjErrorCode to integrate with Tcl's standard error handling mechanisms