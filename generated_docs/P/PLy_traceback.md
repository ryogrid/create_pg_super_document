# PLy_traceback

## Location
[src/pl/plpython/plpy_elog.c:173-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L173-L356)

## Overview
PLy_traceback extracts and formats Python exception information and traceback data into PostgreSQL-compatible error message strings, providing detailed debugging information for PL/Python errors.

## Definition

```c
static void
PLy_traceback(PyObject *e, PyObject *v, PyObject *tb,
			  char *volatile *xmsg, char *volatile *tbmsg, int *tb_depth)
```
## Detailed Description
PLy_traceback is a static function that processes Python exception objects to create formatted error messages and traceback strings for PostgreSQL error reporting. The function operates in two phases:

1. **Exception Message Formatting**: Extracts the exception type and module information, formats them similar to Python's traceback.format_exception_only(), and creates a readable error message string.

2. **Traceback Processing**: Walks through the Python traceback object chain, extracting frame information (filename, line number, function name) and formatting it into a traceback string that mimics Python's standard traceback output.

The function handles special cases like built-in exceptions, PL/Python function contexts, and source code line extraction for better debugging information. It properly manages Python object reference counting and uses PostgreSQL's exception handling for cleanup.

## Parameters / Member Variables
- `*e`: Python exception type object
- `*v`: Python exception value/instance object
- `*tb`: Python traceback object
- `*xmsg`: Output pointer for formatted exception message string (palloc'd)
- `*tbmsg`: Output pointer for formatted traceback string (palloc'd)
- `*tb_depth`: Output pointer for traceback depth count
## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_AsString](PLyUnicode_AsString.md): Converts Python strings to C strings
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current PL/Python execution context
  - [PLy_procedure_name](PLy_procedure_name.md): Gets procedure name from execution context
  - [get_source_line](../g/get_source_line.md): Extracts specific source code line
  - PG_TRY/PG_FINALLY/PG_END_TRY: PostgreSQL exception handling macros
- Called from (representative examples):
  - [PLy_elog_impl](PLy_elog_impl.md): Main error logging function

## Notes and Other Information
- Function mimics Python's traceback module behavior for consistency
- Skips the first frame (module level) and handles the second frame specially  
- Extracts actual source code lines for PL/Python functions when available
- Properly handles Python object cleanup with Py_XDECREF in PG_FINALLY blocks
- Uses StringInfo for efficient string building
- Returns NULL values when no exception is present
- Handles both built-in and custom exception modules appropriately
- Function is located in src/pl/plpython/plpy_elog.c:173-356

## Simplified Source

```c
static void PLy_traceback(PyObject *e, PyObject *v, PyObject *tb,
                         char *volatile *xmsg, char *volatile *tbmsg, int *tb_depth) {
    PyObject *volatile e_type_o = NULL;
    PyObject *volatile e_module_o = NULL;
    PyObject *volatile vob = NULL;
    StringInfoData tbstr;

    // Handle null exception case
    if (e == NULL) {
        *xmsg = NULL;
        *tbmsg = NULL;
        *tb_depth = 0;
        return;
    }

    // Format exception message
    PG_TRY(); {
        char *e_type_s = NULL;
        char *e_module_s = NULL;
        const char *vstr;
        StringInfoData xstr;

        // Get exception type and module names
        e_type_o = PyObject_GetAttrString(e, "__name__");
        e_module_o = PyObject_GetAttrString(e, "__module__");
        if (e_type_o)
            e_type_s = PLyUnicode_AsString(e_type_o);
        if (e_module_o)
            e_module_s = PLyUnicode_AsString(e_module_o);

        // Get exception value string
        if (v && ((vob = PyObject_Str(v)) != NULL))
            vstr = PLyUnicode_AsString(vob);
        else
            vstr = "unknown";

        // Build exception message (mimics traceback.format_exception_only)
        initStringInfo(&xstr);
        if (!e_type_s || !e_module_s) {
            appendStringInfoString(&xstr, "unrecognized exception");
        } else if (strcmp(e_module_s, "builtins") == 0 ||
                   strcmp(e_module_s, "__main__") == 0 ||
                   strcmp(e_module_s, "exceptions") == 0) {
            appendStringInfoString(&xstr, e_type_s);
        } else {
            appendStringInfo(&xstr, "%s.%s", e_module_s, e_type_s);
        }
        appendStringInfo(&xstr, ": %s", vstr);

        *xmsg = xstr.data;
    }
    PG_FINALLY(); {
        Py_XDECREF(e_type_o);
        Py_XDECREF(e_module_o);
        Py_XDECREF(vob);
    }
    PG_END_TRY();

    // Format traceback
    *tb_depth = 0;
    initStringInfo(&tbstr);
    appendStringInfoString(&tbstr, "Traceback (most recent call last):");

    // Walk through traceback frames
    while (tb != NULL && tb != Py_None) {
        PyObject *volatile frame = NULL;
        PyObject *volatile code = NULL;
        PyObject *volatile name = NULL;
        PyObject *volatile lineno = NULL;
        PyObject *volatile filename = NULL;

        PG_TRY(); {
            // Extract frame information
            lineno = PyObject_GetAttrString(tb, "tb_lineno");
            frame = PyObject_GetAttrString(tb, "tb_frame");
            code = PyObject_GetAttrString(frame, "f_code");
            name = PyObject_GetAttrString(code, "co_name");
            filename = PyObject_GetAttrString(code, "co_filename");

            // Skip first frame, format subsequent frames
            if (*tb_depth > 0) {
                PLyExecutionContext *exec_ctx = PLy_current_execution_context();
                char *proname;
                char *fname;
                char *plain_filename;
                long plain_lineno;

                // Format function name (special case for second frame)
                if (*tb_depth == 1)
                    fname = "<module>";
                else
                    fname = PLyUnicode_AsString(name);

                proname = PLy_procedure_name(exec_ctx->curr_proc);
                plain_filename = PLyUnicode_AsString(filename);
                plain_lineno = PyLong_AsLong(lineno);

                // Add frame info to traceback
                if (proname == NULL)
                    appendStringInfo(&tbstr, "\n  PL/Python anonymous code block, line %ld, in %s",
                                   plain_lineno - 1, fname);
                else
                    appendStringInfo(&tbstr, "\n  PL/Python function \"%s\", line %ld, in %s",
                                   proname, plain_lineno - 1, fname);

                // Add source line if available and from <string> (compiled function)
                if (exec_ctx->curr_proc && plain_filename != NULL &&
                    strcmp(plain_filename, "<string>") == 0) {
                    char *line = get_source_line(exec_ctx->curr_proc->src, plain_lineno);
                    if (line) {
                        appendStringInfo(&tbstr, "\n    %s", line);
                        pfree(line);
                    }
                }
            }
        }
        PG_FINALLY(); {
            Py_XDECREF(frame);
            Py_XDECREF(code);
            Py_XDECREF(name);
            Py_XDECREF(lineno);
            Py_XDECREF(filename);
        }
        PG_END_TRY();

        // Move to next frame
        tb = PyObject_GetAttrString(tb, "tb_next");
        (*tb_depth)++;
    }

    *tbmsg = tbstr.data;
}
```