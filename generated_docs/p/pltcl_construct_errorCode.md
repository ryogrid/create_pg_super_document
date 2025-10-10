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
  - [unpack_sql_state](../u/unpack_sql_state.md)
  - [pltcl_get_condition_name](pltcl_get_condition_name.md)
  - UTF_E2U
- Called from (representative examples):
  - [pltcl_elog](pltcl_elog.md)
  - [pltcl_subtrans_abort](pltcl_subtrans_abort.md)
  - [pltcl_commit](pltcl_commit.md)
  - [pltcl_rollback](pltcl_rollback.md)

## Notes and Other Information
- Creates errorCode as a flat list with alternating keys and values for easy Tcl processing
- Always includes mandatory fields: "POSTGRES", version, "SQLSTATE", state code, "condition", condition name, "message", error message
- Conditionally includes optional fields only when present in ErrorData: detail, hint, context, schema, table, column, datatype, constraint, statement, cursor_position, filename, lineno, funcname
- Uses UTF_E2U conversion for all string values to ensure proper UTF-8 encoding for Tcl
- Handles both string and integer values appropriately (cursor_position and lineno as integers)
- Excludes cursorpos field in favor of internalpos for more relevant error positioning information
- Provides complete error context for sophisticated error handling and debugging in PL/Tcl procedures
- Sets the errorCode using Tcl_SetObjErrorCode to integrate with Tcl's standard error handling mechanisms

## Simplified Source

```c
static void pltcl_construct_errorCode(Tcl_Interp *interp, ErrorData *edata) {
    Tcl_Obj *obj = Tcl_NewObj();

    // Build structured errorCode list with mandatory fields
    Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("POSTGRES", -1));
    Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj(PG_VERSION, -1));
    Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("SQLSTATE", -1));
    Tcl_ListObjAppendElement(interp, obj,
                            Tcl_NewStringObj(unpack_sql_state(edata->sqlerrcode), -1));
    Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("condition", -1));
    Tcl_ListObjAppendElement(interp, obj,
                            Tcl_NewStringObj(pltcl_get_condition_name(edata->sqlerrcode), -1));
    Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("message", -1));
    UTF_BEGIN;
    Tcl_ListObjAppendElement(interp, obj,
                            Tcl_NewStringObj(UTF_E2U(edata->message), -1));
    UTF_END;

    // Add optional context fields when available
    if (edata->detail) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("detail", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->detail), -1));
        UTF_END;
    }

    if (edata->hint) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("hint", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->hint), -1));
        UTF_END;
    }

    if (edata->context) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("context", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->context), -1));
        UTF_END;
    }

    // Add schema/table/column information if available
    if (edata->schema_name) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("schema", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->schema_name), -1));
        UTF_END;
    }

    if (edata->table_name) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("table", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->table_name), -1));
        UTF_END;
    }

    if (edata->column_name) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("column", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->column_name), -1));
        UTF_END;
    }

    // Add constraint and source location information
    if (edata->constraint_name) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("constraint", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->constraint_name), -1));
        UTF_END;
    }

    if (edata->internalquery) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("statement", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->internalquery), -1));
        UTF_END;
    }

    if (edata->funcname) {
        Tcl_ListObjAppendElement(interp, obj, Tcl_NewStringObj("funcname", -1));
        UTF_BEGIN;
        Tcl_ListObjAppendElement(interp, obj,
                                Tcl_NewStringObj(UTF_E2U(edata->funcname), -1));
        UTF_END;
    }

    // Set the constructed errorCode in the interpreter
    Tcl_SetObjErrorCode(interp, obj);
}
```