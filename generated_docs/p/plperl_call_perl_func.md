# plperl_call_perl_func

## Location
[src/pl/plperl/plperl.c:2180-2272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2180-L2272)

## Overview
This function is the core execution engine for calling Perl functions from PostgreSQL, converting PostgreSQL function arguments to Perl values and executing the Perl code.

## Definition
```c
static SV *plperl_call_perl_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo)
```

## Detailed Description
`plperl_call_perl_func` serves as the bridge between PostgreSQL and Perl, handling the complex task of converting PostgreSQL data types to Perl scalar values (SV) and executing the compiled Perl function. The function performs argument type conversion based on the function signature, supports various PostgreSQL data types including arrays, composite types, and transform functions, then calls the Perl subroutine and handles any errors that occur during execution.

The function uses Perl's XS API extensively, managing the Perl stack for argument passing and return value handling. It properly handles NULL values by converting them to Perl's undef, converts row types to Perl hash references, and handles array types through specialized conversion functions.

## Parameters / Member Variables
- `desc`: Pointer to plperl_proc_desc structure containing compiled Perl function metadata and argument type information
- `fcinfo`: Standard PostgreSQL FunctionCallInfo structure containing function arguments, null flags, and function metadata

## Dependencies
- Functions called/Symbols referenced:
  - [get_func_signature](../g/get_func_signature.md)
  - [plperl_hash_from_datum](plperl_hash_from_datum.md)
  - [plperl_ref_from_pg_array](plperl_ref_from_pg_array.md)
  - [get_transform_fromsql](../g/get_transform_fromsql.md)
  - OidFunctionCall1
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [cstr2sv](../c/cstr2sv.md)
  - call_sv
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md)
- Called from:
  - [plperl_inline_handler](plperl_inline_handler.md)
  - [plperl_func_handler](plperl_func_handler.md)

## Notes and Other Information
- Uses Perl XS macros (dTHX, dSP, ENTER, SAVETMPS, etc.) for proper Perl API integration
- Handles three main argument conversion paths: NULL values, row types (converted to hash), and scalar types
- Supports PostgreSQL transform functions for custom type conversions
- Uses G_SCALAR | G_EVAL flags when calling Perl subroutine to ensure scalar context and error handling
- Performs comprehensive error checking including return count validation and Perl error (ERRSV) handling
- Memory management follows Perl conventions with proper mortal SV handling and cleanup

## Simplified Source

```c
static SV *plperl_call_perl_func(plperl_proc_desc *desc, FunctionCallInfo fcinfo) {
    dTHX;
    dSP;
    SV *retval;
    int i, count;
    Oid *argtypes = NULL;
    int nargs = 0;

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    EXTEND(sp, desc->nargs);

    // Get function signature for argument type information
    if (fcinfo->flinfo->fn_oid)
        get_func_signature(fcinfo->flinfo->fn_oid, &argtypes, &nargs);

    // Convert PostgreSQL arguments to Perl values
    for (i = 0; i < desc->nargs; i++) {
        if (fcinfo->args[i].isnull) {
            // NULL values become Perl undef
            PUSHs(&PL_sv_undef);
        } else if (desc->arg_is_rowtype[i]) {
            // Row types become Perl hash references
            SV *sv = plperl_hash_from_datum(fcinfo->args[i].value);
            PUSHs(sv_2mortal(sv));
        } else {
            // Handle scalar types with various conversion methods
            SV *sv;
            Oid funcid;

            if (OidIsValid(desc->arg_arraytype[i])) {
                // Convert arrays using specialized function
                sv = plperl_ref_from_pg_array(fcinfo->args[i].value, desc->arg_arraytype[i]);
            } else if ((funcid = get_transform_fromsql(argtypes[i],
                                                      current_call_data->prodesc->lang_oid,
                                                      current_call_data->prodesc->trftypes))) {
                // Use transform function if available
                sv = (SV *) DatumGetPointer(OidFunctionCall1(funcid, fcinfo->args[i].value));
            } else {
                // Default: convert to string and then to Perl scalar
                char *tmp = OutputFunctionCall(&(desc->arg_out_func[i]), fcinfo->args[i].value);
                sv = cstr2sv(tmp);
                pfree(tmp);
            }
            PUSHs(sv_2mortal(sv));
        }
    }
    PUTBACK;

    // Call the Perl subroutine
    count = call_sv(desc->reference, G_SCALAR | G_EVAL);
    SPAGAIN;

    // Validate return value
    if (count != 1) {
        PUTBACK;
        FREETMPS;
        LEAVE;
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("didn't get a return item from function")));
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

    // Extract and copy return value
    retval = newSVsv(POPs);

    PUTBACK;
    FREETMPS;
    LEAVE;

    return retval;
}
```