# plperl_create_sub

## Location
[src/pl/plperl/plperl.c:2095-2167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L2095-L2167)

## Overview
Creates a Perl subroutine from source code text and stores a reference to it in the procedure descriptor for later execution.

## Definition
```c
static void plperl_create_sub(plperl_proc_desc *prodesc, const char *s, Oid fn_oid)
```

## Detailed Description
This function compiles Perl source code into an executable subroutine using PostgreSQL's internal Perl infrastructure. It generates a unique subroutine name based on the function name and OID, sets up appropriate pragma directives (like strict mode if enabled), and calls the PostgreSQL::InServer::mkfunc function to perform the actual compilation. The resulting code reference is stored in the procedure descriptor for later execution. The function handles compilation errors and ensures that a valid code reference is obtained before returning.

## Parameters / Member Variables
- `prodesc`: Pointer to the procedure descriptor structure that will store the compiled subroutine reference
- `s`: The Perl source code text to compile into a subroutine
- `fn_oid`: The function OID used to generate a unique subroutine name

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading macro)
  - NAMEDATALEN (PostgreSQL constant)
  - [hv_store_string](../h/hv_store_string.md)
  - [cstr2sv](../c/cstr2sv.md)
  - newRV_noinc
  - PL_sv_no
  - call_pv (calls PostgreSQL::InServer::mkfunc)
  - newRV_inc
  - ERRSV
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md)
- Called from (representative examples):
  - [plperl_inline_handler](plperl_inline_handler.md)
  - [compile_plperl_function](../c/compile_plperl_function.md)

## Notes and Other Information
- Generates unique subroutine names using format: `{proname}__{fn_oid}`
- Respects the plperl_use_strict setting by adding strict pragma when enabled
- Uses 'false' for $prolog parameter in mkfunc for compatibility with modules like PostgreSQL::PLPerl::NYTprof
- Employs G_KEEPERR flag to properly recognize compilation errors
- Located in src/pl/plperl/plperl.c:2095-2167
- The compiled subroutine reference is stored in prodesc->reference for later execution
- Reports syntax errors with appropriate error codes if compilation fails

## Simplified Source

```c
static void plperl_create_sub(plperl_proc_desc *prodesc, const char *s, Oid fn_oid) {
    // Create unique subroutine name: {proname}__{fn_oid}
    char subname[NAMEDATALEN + 40];
    sprintf(subname, "%s__%u", prodesc->proname, fn_oid);

    // Set up pragma hash (add 'strict' if enabled)
    HV *pragma_hv = newHV();
    if (plperl_use_strict)
        hv_store_string(pragma_hv, "strict", (SV *) newAV());

    // Prepare Perl stack and call mkfunc to compile the code
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    EXTEND(SP, 4);
    PUSHs(sv_2mortal(cstr2sv(subname)));        // subroutine name
    PUSHs(sv_2mortal(newRV_noinc((SV *) pragma_hv))); // pragmas
    PUSHs(&PL_sv_no);                           // prolog = false
    PUSHs(sv_2mortal(cstr2sv(s)));              // source code
    PUTBACK;

    // Call PostgreSQL::InServer::mkfunc to compile
    int count = call_pv("PostgreSQL::InServer::mkfunc", G_SCALAR | G_EVAL | G_KEEPERR);
    SPAGAIN;

    // Extract compiled subroutine reference
    SV *subref = NULL;
    if (count == 1) {
        SV *sub_rv = (SV *) POPs;
        if (sub_rv && SvROK(sub_rv) && SvTYPE(SvRV(sub_rv)) == SVt_PVCV) {
            subref = newRV_inc(SvRV(sub_rv));
        }
    }

    PUTBACK;
    FREETMPS;
    LEAVE;

    // Check for compilation errors
    if (SvTRUE(ERRSV))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("%s", strip_trailing_ws(sv2cstr(ERRSV)))));

    if (!subref)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("didn't get a CODE reference from compiling function \"%s\"",
                              prodesc->proname)));

    // Store compiled subroutine reference for later execution
    prodesc->reference = subref;
}
```