# plperl_call_perl_event_trigger_func

## Location
[src/pl/plperl/plperl.c:2341-2401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2341-L2401)

## Overview
This function executes Perl event trigger functions in PostgreSQL, providing access to DDL command information through the $_TD global variable.

## Definition
```c
static void plperl_call_perl_event_trigger_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo, SV *td)
```

## Detailed Description
`plperl_call_perl_event_trigger_func` handles the execution of Perl event trigger functions, which are triggered by DDL commands (CREATE, ALTER, DROP, etc.) rather than DML operations on tables. Unlike regular triggers, event triggers do not take arguments and do not return values that affect command execution. The function sets up the special `$_TD` global variable containing event information and executes the Perl subroutine.

Event triggers are primarily used for auditing, logging, or enforcing policies on DDL operations. The function provides a simplified execution model compared to regular triggers since event triggers cannot modify the DDL command or its results.

## Parameters / Member Variables
- `desc`: Pointer to plperl_proc_desc structure containing the compiled Perl event trigger function reference
- `fcinfo`: Standard PostgreSQL FunctionCallInfo structure containing event trigger context
- `td`: Pre-constructed Perl hash reference containing event trigger data (accessible as $_TD in Perl)

## Dependencies
- Functions called/Symbols referenced:
  - get_sv (to access main::_TD global variable)
  - save_item (for local variable scoping)
  - call_sv (executes the Perl subroutine)
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md)
- Called from:
  - [plperl_event_trigger_handler](plperl_event_trigger_handler.md)

## Notes and Other Information
- Return type is void since event triggers cannot affect command execution
- No arguments are passed to the Perl function (unlike regular triggers)
- Sets up $_TD containing event information like command tag, object type, schema name
- Uses local scoping for $_TD to avoid conflicts with nested calls
- The return value is captured but discarded (with compiler warning suppression)
- Error handling follows standard Perl XS patterns with proper exception propagation
- Designed for DDL event monitoring and auditing rather than data modification control

## Simplified Source

```c
static void plperl_call_perl_event_trigger_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo, SV *td) {
    dTHX;
    dSP;
    SV *retval, *TDsv;
    int count;

    ENTER;
    SAVETMPS;

    // Set up global $_TD variable with event trigger data
    TDsv = get_sv("main::_TD", 0);
    if (!TDsv)
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("couldn't fetch $_TD")));

    save_item(TDsv);            // Local $_TD scope
    sv_setsv(TDsv, td);         // Set $_TD to event data hash

    // Prepare Perl stack (no arguments for event triggers)
    PUSHMARK(sp);
    PUTBACK;

    // Call the Perl event trigger function
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

    // Extract return value (discarded for event triggers)
    retval = newSVsv(POPs);
    (void) retval;              // Silence compiler warning

    PUTBACK;
    FREETMPS;
    LEAVE;
}
```