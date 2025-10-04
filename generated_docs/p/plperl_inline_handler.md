# plperl_inline_handler

## Location
[src/pl/plperl/plperl.c:1894-1988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1894-L1988)

## Overview
Handles execution of anonymous Perl code blocks (DO statements) by creating a temporary function environment and executing the provided code.

## Definition

```c
Datum
plperl_inline_handler(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL DO statement functionality for PL/Perl by executing anonymous code blocks. It creates a synthetic function environment with fake function call information and procedure descriptor to reuse the existing PL/Perl function execution infrastructure. The function sets up error handling context, manages SPI connections for database access, creates a temporary Perl subroutine from the source code, and executes it. The implementation carefully manages memory and references, ensuring proper cleanup even in error conditions using PostgreSQL's exception handling framework.

## Parameters / Member Variables
- Implicit  parameter (accessed via PG_GETARG_POINTER): InlineCodeBlock structure containing the Perl source code and language metadata

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro to create local function call info)
  - [InlineCodeBlock](../I/InlineCodeBlock.md) (structure containing code block information)
  - [plperl_proc_desc](plperl_proc_desc.md) (procedure descriptor structure)
  - [plperl_call_data](plperl_call_data.md) (call state management structure)
  - MemSet (memory initialization)
  - [plperl_inline_callback](plperl_inline_callback.md) (error context callback)
  - SizeForFunctionCallInfo (size calculation for fcinfo)
  - [SPI_connect_ext](../S/SPI_connect_ext.md), SPI_finish (SPI database interface)
  - SPI_OPT_NONATOMIC (non-atomic SPI option)
  - [select_perl_context](../s/select_perl_context.md) (Perl interpreter context selection)
  - [plperl_create_sub](plperl_create_sub.md) (create Perl subroutine)
  - [plperl_call_perl_func](plperl_call_perl_func.md) (execute Perl function)
  - [SvREFCNT_dec_current](../S/SvREFCNT_dec_current.md) (Perl reference counting)
  - [activate_interpreter](../a/activate_interpreter.md) (interpreter management)
  - PG_TRY, PG_FINALLY, PG_END_TRY (exception handling)
- Called from (representative examples):
  - [plperlu_inline_handler](plperlu_inline_handler.md)

## Notes and Other Information
- Creates a fake function call environment to reuse existing PL/Perl infrastructure
- Sets procedure name to 'inline_code_block' for error reporting
- Handles both trusted and untrusted Perl contexts based on language settings
- Manages SPI connections with atomic/non-atomic behavior based on code block settings
- Uses error context callbacks for better error reporting during inline code execution
- Properly manages Perl reference counting for temporary subroutines and return values
- Returns void as inline code blocks don't return values to SQL level
- Essential for PostgreSQL's DO statement functionality in PL/Perl

## Simplified Source

```c
Datum plperl_inline_handler(PG_FUNCTION_ARGS)
{
    LOCAL_FCINFO(fake_fcinfo, 0);
    InlineCodeBlock *codeblock = (InlineCodeBlock *) PG_GETARG_POINTER(0);
    FmgrInfo flinfo;
    plperl_proc_desc desc;
    plperl_call_data *volatile save_call_data = current_call_data;
    plperl_interp_desc *volatile oldinterp = plperl_active_interp;
    plperl_call_data this_call_data;
    ErrorContextCallback pl_error_context;

    // Initialize current call status record
    MemSet(&this_call_data, 0, sizeof(this_call_data));

    // Set up error reporting callback
    pl_error_context.callback = plperl_inline_callback;
    pl_error_context.previous = error_context_stack;
    pl_error_context.arg = NULL;
    error_context_stack = &pl_error_context;

    // Create fake function call info for inline code execution
    MemSet(fake_fcinfo, 0, SizeForFunctionCallInfo(0));
    MemSet(&flinfo, 0, sizeof(flinfo));
    MemSet(&desc, 0, sizeof(desc));

    fake_fcinfo->flinfo = &flinfo;
    flinfo.fn_oid = InvalidOid;
    flinfo.fn_mcxt = CurrentMemoryContext;

    // Set up procedure descriptor for inline code
    desc.proname = "inline_code_block";
    desc.fn_readonly = false;
    desc.lang_oid = codeblock->langOid;
    desc.trftypes = NIL;
    desc.lanpltrusted = codeblock->langIsTrusted;
    desc.fn_retistuple = false;
    desc.fn_retisset = false;
    desc.fn_retisarray = false;
    desc.result_oid = InvalidOid;
    desc.nargs = 0;
    desc.reference = NULL;

    this_call_data.fcinfo = fake_fcinfo;
    this_call_data.prodesc = &desc;

    PG_TRY();
    {
        SV *perlret;

        current_call_data = &this_call_data;

        // Connect to SPI for database access
        if (SPI_connect_ext(codeblock->atomic ? 0 : SPI_OPT_NONATOMIC) != SPI_OK_CONNECT)
            elog(ERROR, "could not connect to SPI manager");

        // Select appropriate Perl context (trusted/untrusted)
        select_perl_context(desc.lanpltrusted);

        // Create and execute the Perl subroutine
        plperl_create_sub(&desc, codeblock->source_text, 0);

        if (!desc.reference)
            elog(ERROR, "could not create internal procedure for anonymous code block");

        perlret = plperl_call_perl_func(&desc, fake_fcinfo);
        SvREFCNT_dec_current(perlret);

        if (SPI_finish() != SPI_OK_FINISH)
            elog(ERROR, "SPI_finish() failed");
    }
    PG_FINALLY();
    {
        // Clean up resources
        if (desc.reference)
            SvREFCNT_dec_current(desc.reference);
        current_call_data = save_call_data;
        activate_interpreter(oldinterp);
    }
    PG_END_TRY();

    error_context_stack = pl_error_context.previous;

    PG_RETURN_VOID();
}
```