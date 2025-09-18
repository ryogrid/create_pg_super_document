# plperl_inline_handler

## Location
src/pl/plperl/plperl.c: 1894 - 1988

## Overview
Handles execution of anonymous Perl code blocks (DO statements) by creating a temporary function environment and executing the provided code.

## Definition


## Detailed Description
This function implements the PostgreSQL DO statement functionality for PL/Perl by executing anonymous code blocks. It creates a synthetic function environment with fake function call information and procedure descriptor to reuse the existing PL/Perl function execution infrastructure. The function sets up error handling context, manages SPI connections for database access, creates a temporary Perl subroutine from the source code, and executes it. The implementation carefully manages memory and references, ensuring proper cleanup even in error conditions using PostgreSQL's exception handling framework.

## Parameters / Member Variables
- Implicit  parameter (accessed via PG_GETARG_POINTER): InlineCodeBlock structure containing the Perl source code and language metadata

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro to create local function call info)
  - InlineCodeBlock (structure containing code block information)
  - plperl_proc_desc (procedure descriptor structure)
  - plperl_call_data (call state management structure)
  - MemSet (memory initialization)
  - plperl_inline_callback (error context callback)
  - SizeForFunctionCallInfo (size calculation for fcinfo)
  - SPI_connect_ext, SPI_finish (SPI database interface)
  - SPI_OPT_NONATOMIC (non-atomic SPI option)
  - select_perl_context (Perl interpreter context selection)
  - plperl_create_sub (create Perl subroutine)
  - plperl_call_perl_func (execute Perl function)
  - SvREFCNT_dec_current (Perl reference counting)
  - activate_interpreter (interpreter management)
  - PG_TRY, PG_FINALLY, PG_END_TRY (exception handling)
- Called from (representative examples):
  - plperlu_inline_handler

## Notes and Other Information
- Creates a fake function call environment to reuse existing PL/Perl infrastructure
- Sets procedure name to 'inline_code_block' for error reporting
- Handles both trusted and untrusted Perl contexts based on language settings
- Manages SPI connections with atomic/non-atomic behavior based on code block settings
- Uses error context callbacks for better error reporting during inline code execution
- Properly manages Perl reference counting for temporary subroutines and return values
- Returns void as inline code blocks don't return values to SQL level
- Essential for PostgreSQL's DO statement functionality in PL/Perl