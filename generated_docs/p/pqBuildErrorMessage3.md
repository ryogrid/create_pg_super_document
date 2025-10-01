# pqBuildErrorMessage3

## Location
[src/interfaces/libpq/fe-protocol3.c:1014-1184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1014-L1184)

## Overview
Constructs a formatted error message from the fields in a PGresult structure, with support for different verbosity levels and context visibility options.

## Definition

```c
struction.
 *
 * The cursor location is measured in logical characters;
```
## Detailed Description
This function builds a comprehensive error message by extracting and formatting various diagnostic fields from a PGresult structure. It supports three verbosity levels: TERSE (minimal), DEFAULT (standard), and VERBOSE (detailed). The function handles different types of error position information (statement position and internal position), formats query text with syntax cursor display when appropriate, and includes additional diagnostic information based on the verbosity setting.

The function processes error fields in a specific order: severity, SQLSTATE (if verbose), primary message, position information, and then additional details like DETAIL, HINT, QUERY, CONTEXT, and schema/table/column names. For VERBOSE mode, it also includes source location information (file, line, function).

## Parameters / Member Variables
- : PQExpBuffer to append the formatted error message to
- : PGresult structure containing the error fields to format
- : Controls the amount of detail in the error message (PQERRORS_TERSE, PQERRORS_DEFAULT, PQERRORS_VERBOSE, PQERRORS_SQLSTATE)
- : Controls when to show context information (PQSHOW_CONTEXT_NEVER, PQSHOW_CONTEXT_ERRORS, PQSHOW_CONTEXT_ALWAYS)

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - [PQresultErrorField](../P/PQresultErrorField.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - atoi
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - reportErrorPosition
- Called from (representative examples):
  - [PQresultVerboseErrorMessage](../P/PQresultVerboseErrorMessage.md)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md)

## Notes and Other Information
- Handles NULL PGresult by returning "out of memory" message
- Falls back to base error message if no broken-down fields are available
- Supports internationalization through libpq_gettext for translatable strings
- Handles both statement position and internal position error reporting
- [Query](../Q/Query.md) text with syntax cursor display is shown only for non-TERSE verbosity levels
- VERBOSE mode includes schema, table, column, datatype, and constraint names when available
- Source location information (file:line, function) is included only in VERBOSE mode
- Context information display is controlled by show_context parameter and result status

## Simplified Source

```c
void
pqBuildErrorMessage3(PQExpBuffer msg, const PGresult *res,
                    PGVerbosity verbosity, PGContextVisibility show_context)
{
    const char *val;
    const char *querytext = NULL;
    int querypos = 0;

    // Handle NULL result
    if (res == NULL) {
        appendPQExpBufferStr(msg, libpq_gettext("out of memory\n"));
        return;
    }

    // If no broken-down fields, return base message
    if (res->errFields == NULL) {
        if (res->errMsg && res->errMsg[0])
            appendPQExpBufferStr(msg, res->errMsg);
        else
            appendPQExpBufferStr(msg, libpq_gettext("no error message available\n"));
        return;
    }

    // Add severity
    val = PQresultErrorField(res, PG_DIAG_SEVERITY);
    if (val)
        appendPQExpBuffer(msg, "%s:  ", val);

    // Handle SQLSTATE-only mode
    if (verbosity == PQERRORS_SQLSTATE) {
        val = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (val) {
            appendPQExpBuffer(msg, "%s\n", val);
            return;
        }
        verbosity = PQERRORS_TERSE;  // fallback
    }

    // Add SQLSTATE for verbose mode
    if (verbosity == PQERRORS_VERBOSE) {
        val = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (val)
            appendPQExpBuffer(msg, "%s: ", val);
    }

    // Add primary message
    val = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    if (val)
        appendPQExpBufferStr(msg, val);

    // Handle position information
    val = PQresultErrorField(res, PG_DIAG_STATEMENT_POSITION);
    if (val) {
        if (verbosity != PQERRORS_TERSE && res->errQuery != NULL) {
            querytext = res->errQuery;
            querypos = atoi(val);
        } else {
            appendPQExpBuffer(msg, libpq_gettext(" at character %s"), val);
        }
    } else {
        val = PQresultErrorField(res, PG_DIAG_INTERNAL_POSITION);
        if (val) {
            querytext = PQresultErrorField(res, PG_DIAG_INTERNAL_QUERY);
            if (verbosity != PQERRORS_TERSE && querytext != NULL) {
                querypos = atoi(val);
            } else {
                appendPQExpBuffer(msg, libpq_gettext(" at character %s"), val);
            }
        }
    }

    appendPQExpBufferChar(msg, '\n');

    // Add detailed information for non-terse modes
    if (verbosity != PQERRORS_TERSE) {
        if (querytext && querypos > 0)
            reportErrorPosition(msg, querytext, querypos, res->client_encoding);

        // Add DETAIL, HINT, QUERY if available
        val = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("DETAIL:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("HINT:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_INTERNAL_QUERY);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("QUERY:  %s\n"), val);

        // Add context if requested
        if (show_context == PQSHOW_CONTEXT_ALWAYS ||
            (show_context == PQSHOW_CONTEXT_ERRORS && res->resultStatus == PGRES_FATAL_ERROR)) {
            val = PQresultErrorField(res, PG_DIAG_CONTEXT);
            if (val)
                appendPQExpBuffer(msg, libpq_gettext("CONTEXT:  %s\n"), val);
        }
    }

    // Add verbose schema/table/column information
    if (verbosity == PQERRORS_VERBOSE) {
        val = PQresultErrorField(res, PG_DIAG_SCHEMA_NAME);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("SCHEMA NAME:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_TABLE_NAME);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("TABLE NAME:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_COLUMN_NAME);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("COLUMN NAME:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_DATATYPE_NAME);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("DATATYPE NAME:  %s\n"), val);

        val = PQresultErrorField(res, PG_DIAG_CONSTRAINT_NAME);
        if (val)
            appendPQExpBuffer(msg, libpq_gettext("CONSTRAINT NAME:  %s\n"), val);

        // Add source location information
        const char *valf = PQresultErrorField(res, PG_DIAG_SOURCE_FILE);
        const char *vall = PQresultErrorField(res, PG_DIAG_SOURCE_LINE);
        val = PQresultErrorField(res, PG_DIAG_SOURCE_FUNCTION);
        if (val || valf || vall) {
            appendPQExpBufferStr(msg, libpq_gettext("LOCATION:  "));
            if (val)
                appendPQExpBuffer(msg, libpq_gettext("%s, "), val);
            if (valf && vall)
                appendPQExpBuffer(msg, libpq_gettext("%s:%s"), valf, vall);
            appendPQExpBufferChar(msg, '\n');
        }
    }
}
```