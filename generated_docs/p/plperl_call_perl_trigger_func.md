# plperl_call_perl_trigger_func

## Location
src/pl/plperl/plperl.c: 2273 - 2340

## Overview
This function executes Perl trigger functions in PostgreSQL, setting up the trigger data context and passing trigger arguments to the Perl subroutine.

## Definition
```c
static SV *plperl_call_perl_trigger_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo, SV *td)
```

## Detailed Description
`plperl_call_perl_trigger_func` is specialized for executing Perl trigger functions within PostgreSQL's trigger system. It sets up the special Perl global variable `$_TD` (Trigger Data) that contains trigger context information, then calls the Perl trigger function with the trigger's custom arguments. The function follows the PostgreSQL trigger calling convention where trigger functions receive their arguments as an array and return values that control trigger behavior.

The function manages the Perl execution environment by temporarily setting the global `$_TD` variable to the provided trigger data hash reference, executes the Perl trigger subroutine with the trigger's arguments, and handles any errors that occur during execution.

## Parameters / Member Variables
- `desc`: Pointer to plperl_proc_desc structure containing the compiled Perl trigger function reference
- `fcinfo`: Standard PostgreSQL FunctionCallInfo structure containing trigger context data
- `td`: Pre-constructed Perl hash reference containing trigger data (accessible as $_TD in Perl)

## Dependencies
- Functions called/Symbols referenced:
  - get_sv (to access main::_TD global variable)
  - save_item (for local variable scoping)
  - [cstr2sv](../c/cstr2sv.md) (converts trigger arguments to Perl strings)
  - call_sv (executes the Perl subroutine)
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md)
- Called from:
  - [plperl_trigger_handler](plperl_trigger_handler.md)

## Notes and Other Information
- Sets up the global Perl variable `$_TD` containing trigger metadata (table name, operation type, etc.)
- Passes trigger arguments (tgargs) as individual string parameters to the Perl function
- Uses local scoping for $_TD to avoid interfering with nested trigger calls
- Return value controls trigger behavior (undef = proceed, "SKIP" = cancel operation, hash = modify row)
- Error handling includes both return count validation and Perl exception checking
- Follows standard Perl XS memory management with proper cleanup of temporary values