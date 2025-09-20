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
  - InlineCodeBlock (structure containing code block information)
  - [plperl_proc_desc](plperl_proc_desc.md) (procedure descriptor structure)
  - [plperl_call_data](plperl_call_data.md) (call state management structure)
  - MemSet (memory initialization)
  - [plperl_inline_callback](plperl_inline_callback.md) (error context callback)
  - SizeForFunctionCallInfo (size calculation for fcinfo)
  - SPI_connect_ext, SPI_finish (SPI database interface)
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