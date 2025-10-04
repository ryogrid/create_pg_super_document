# plpython3_inline_handler

## Location
[src/pl/plpython/plpy_main.c:263-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L263-L338)

## Overview
The plpython3_inline_handler function is responsible for executing inline Python code blocks (DO blocks) in PostgreSQL's PL/Python language handler.

## Definition
Datum plpython3_inline_handler(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the entry point for executing inline Python code blocks submitted via PostgreSQL's DO statement. It creates a temporary procedure context, compiles the Python source code, and executes it within a properly managed execution environment. The function handles both atomic and non-atomic execution contexts depending on the inline code block's requirements.

The function performs comprehensive setup including SPI connection establishment, memory context management, error handling setup, and proper cleanup of resources after execution. It uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to ensure proper cleanup even when errors occur during execution.

## Parameters / Member Variables
- The InlineCodeBlock contains:
  - : The Python code to execute
  - : Language identifier for the Python language handler  
  - : Boolean indicating whether the block should execute atomically

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_initialize](../P/PLy_initialize.md): Initializes the Python language environment
  - [SPI_connect_ext](../S/SPI_connect_ext.md): Establishes SPI connection for database access
  - AllocSetContextCreate: Creates memory context for the procedure
  - [PLy_push_execution_context](../P/PLy_push_execution_context.md): Sets up execution context stack
  - [PLy_procedure_compile](../P/PLy_procedure_compile.md): Compiles the Python source code
  - [PLy_exec_function](../P/PLy_exec_function.md): Executes the compiled Python procedure
  - [PLy_pop_execution_context](../P/PLy_pop_execution_context.md): Cleans up execution context
  - [PLy_procedure_delete](../P/PLy_procedure_delete.md): Deallocates procedure resources
  - [plpython_inline_error_callback](plpython_inline_error_callback.md): Error callback for inline execution
- Called from (representative examples):
  - Called directly by PostgreSQL's function call mechanism for DO blocks

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c:263-338
- Creates a temporary procedure named "__plpython_inline_block" for execution
- Uses SPI_OPT_NONATOMIC flag for non-atomic blocks to allow transaction control
- Implements proper exception handling to ensure cleanup of Python state and memory contexts
- The function always returns void (PG_RETURN_VOID) as inline blocks don't produce return values
- Note in code mentions that SPI_finish() happens in plpy_exec.c, which is described as "dubious design"

## Simplified Source

```c
Datum
plpython3_inline_handler(PG_FUNCTION_ARGS)
{
    LOCAL_FCINFO(fake_fcinfo, 0);
    InlineCodeBlock *codeblock = (InlineCodeBlock *) DatumGetPointer(PG_GETARG_DATUM(0));
    FmgrInfo flinfo;
    PLyProcedure proc;
    PLyExecutionContext *exec_ctx;
    ErrorContextCallback plerrcontext;

    // Initialize PL/Python environment
    PLy_initialize();

    // Establish SPI connection (atomic or non-atomic based on codeblock)
    if (SPI_connect_ext(codeblock->atomic ? 0 : SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Set up fake function call info for inline execution
    MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));
    MemSet(&flinfo, 0, sizeof(flinfo));
    fake_fcinfo->flinfo = &flinfo;
    flinfo.fn_oid = InvalidOid;
    flinfo.fn_mcxt = CurrentMemoryContext;

    // Initialize procedure structure for inline block
    MemSet(&proc, 0, sizeof(PLyProcedure));
    proc.mcxt = AllocSetContextCreate(TopMemoryContext,
                                      "__plpython_inline_block",
                                      ALLOCSET_DEFAULT_SIZES);
    proc.pyname = MemoryContextStrdup(proc.mcxt, "__plpython_inline_block");
    proc.langid = codeblock->langOid;
    proc.result.typoid = VOIDOID;  // Inline blocks return void

    // Push execution context onto stack
    exec_ctx = PLy_push_execution_context(codeblock->atomic);

    PG_TRY();
    {
        // Set up error callback for inline execution
        plerrcontext.callback = plpython_inline_error_callback;
        plerrcontext.arg = exec_ctx;
        plerrcontext.previous = error_context_stack;
        error_context_stack = &plerrcontext;

        // Compile and execute the Python code
        PLy_procedure_compile(&proc, codeblock->source_text);
        exec_ctx->curr_proc = &proc;
        PLy_exec_function(fake_fcinfo, &proc);
    }
    PG_CATCH();
    {
        // Cleanup on error
        PLy_pop_execution_context();
        PLy_procedure_delete(&proc);
        PyErr_Clear();
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Normal cleanup
    PLy_pop_execution_context();
    PLy_procedure_delete(&proc);

    PG_RETURN_VOID();
}
```