# PLy_procedure_call

## Location
[src/pl/plpython/plpy_exec.c:1062-1103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L1062-L1103)

## Overview
Executes a compiled PL/Python procedure with the provided arguments, handling subtransaction management and error propagation between Python and PostgreSQL.

## Definition

```c
static PyObject *
PLy_procedure_call(PLyProcedure *proc, const char *kargs, PyObject *vargs)
```
## Detailed Description
This function is the core execution engine for PL/Python procedures. It sets up the execution environment by adding the provided arguments to the procedure's global namespace, then executes the compiled Python code using PyEval_EvalCode. The function implements proper subtransaction management by tracking the subtransaction nesting level before execution and ensuring any open subtransactions created during the procedure execution are properly aborted if needed. It uses PostgreSQL's PG_TRY/PG_FINALLY/PG_END_TRY exception handling mechanism to ensure cleanup occurs even if errors are raised. If the Python code returns NULL (indicating an error), the function propagates the Python exception to PostgreSQL using PLy_elog.

## Parameters / Member Variables
- `*proc`: Compiled PL/Python procedure containing code object and global namespace
- `*kargs`: Name of the argument variable to set in the global namespace
- `*vargs`: Python object containing the procedure arguments to be made available to the Python code
## Dependencies
- Functions called/Symbols referenced:
  - PyDict_SetItemString
  - PyEval_EvalCode
  - [PLy_abort_open_subtransactions](PLy_abort_open_subtransactions.md)
  - PLy_elog
  - PG_TRY/PG_FINALLY/PG_END_TRY
  - [list_length](../l/list_length.md)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md)
  - [PLy_exec_trigger](PLy_exec_trigger.md)

## Notes and Other Information
The function includes version-specific handling for different Python versions, using PyCodeObject casting for Python versions prior to 3.2.0. The subtransaction management ensures that any subtransactions opened during procedure execution (via plpy.subtransaction()) are properly handled - if the procedure completes normally, subtransactions can remain open for the calling code to manage, but if an error occurs, all subtransactions opened during this procedure call are aborted. The function follows PostgreSQL's exception handling patterns, making it safe to use within PostgreSQL's memory context and error handling system.

## Simplified Source

```c
static PyObject *
PLy_procedure_call(PLyProcedure *proc, const char *kargs, PyObject *vargs)
{
    PyObject *rv = NULL;
    int save_subxact_level = list_length(explicit_subtransactions);

    // Set up arguments in the procedure's global namespace
    PyDict_SetItemString(proc->globals, kargs, vargs);

    PG_TRY();
    {
        // Execute the compiled Python code
#if PY_VERSION_HEX >= 0x03020000
        rv = PyEval_EvalCode(proc->code, proc->globals, proc->globals);
#else
        rv = PyEval_EvalCode((PyCodeObject *) proc->code, proc->globals, proc->globals);
#endif

        // Verify subtransaction nesting is only increased, not decreased
        Assert(list_length(explicit_subtransactions) >= save_subxact_level);
    }
    PG_FINALLY();
    {
        // Clean up any subtransactions opened during execution
        PLy_abort_open_subtransactions(save_subxact_level);
    }
    PG_END_TRY();

    // Propagate Python errors to PostgreSQL
    if (rv == NULL)
        PLy_elog(ERROR, NULL);

    return rv;
}
```