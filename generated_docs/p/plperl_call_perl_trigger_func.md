# plperl_call_perl_trigger_func

## Location
[src/pl/plperl/plperl.c:2273-2340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2273-L2340)

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

## Simplified Source

```c
static SV *plperl_call_perl_trigger_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo, SV *td) {
    dTHX;
    dSP;
    SV *retval, *TDsv;
    int i, count;
    Trigger *tg_trigger = ((TriggerData *) fcinfo->context)->tg_trigger;

    ENTER;
    SAVETMPS;

    // Set up global $_TD variable with trigger data
    TDsv = get_sv("main::_TD", 0);
    if (!TDsv)
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("couldn't fetch $_TD")));

    save_item(TDsv);            // Local $_TD scope
    sv_setsv(TDsv, td);         // Set $_TD to trigger data hash

    // Prepare Perl stack with trigger arguments
    PUSHMARK(sp);
    EXTEND(sp, tg_trigger->tgnargs);

    // Push each trigger argument as a string
    for (i = 0; i < tg_trigger->tgnargs; i++)
        PUSHs(sv_2mortal(cstr2sv(tg_trigger->tgargs[i])));
    PUTBACK;

    // Call the Perl trigger function
    count = call_sv(desc->reference, G_SCALAR | G_EVAL);
    SPAGAIN;

    // Validate return value
    if (count != 1) {
        PUTBACK;
        FREETMPS;
        LEAVE;
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("didn't get a return item from trigger function")));
    }

    // Check for Perl errors
    if (SvTRUE(ERRSV)) {
        (void) POPs;
        PUTBACK;
        FREETMPS;
        LEAVE;
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("%s", strip_trailing_ws(sv2cstr(ERRSV)))));
    }

    // Extract return value (controls trigger behavior)
    retval = newSVsv(POPs);

    PUTBACK;
    FREETMPS;
    LEAVE;

    return retval;
}
```