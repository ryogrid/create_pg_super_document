# plperl_trusted_init

## Location
[src/pl/plperl/plperl.c:957-1037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L957-L1037)

## Overview
Initializes the current Perl interpreter as a trusted interpreter by setting up security restrictions and loading required modules.

## Definition

```c
static void
plperl_trusted_init(void)
```
## Detailed Description
This function configures a Perl interpreter to run in trusted mode by implementing several security measures:

1. **Security Setup**: Temporarily uses the original require/dofile opcodes during initialization, then switches to safe versions
2. **Module Loading**: Executes PLC_TRUSTED code and forces loading of the utf8 module to prevent runtime errors
3. **Opcode Restriction**: Sets PL_op_mask to restrict dangerous opcodes from being compiled
4. **Extension Prevention**: Deletes the DynaLoader namespace to prevent dynamic extension loading
5. **Cache Invalidation**: Clears various caches to ensure security restrictions take effect
6. **Custom Initialization**: Executes user-defined plperl.on_plperl_init code if configured

The function ensures that Perl code runs in a sandboxed environment where potentially dangerous operations are blocked.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading macro)
  - PL_ppaddr (Perl opcode address table)
  - eval_pv (evaluate Perl code)
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md) (convert Perl scalar to C string)
  - [pp_require_safe](pp_require_safe.md) (safe require opcode handler)
  - isGV_with_GP (check if glob has GP structure)
  - GvCV_set (set code value in glob)
  - ereport/errcode/errmsg/errcontext (PostgreSQL error reporting)
- Called from:
  - [select_perl_context](../s/select_perl_context.md) (when setting up trusted Perl context)

## Notes and Other Information
- This function is critical for PL/Perl security as it implements the trusted execution environment
- The utf8 module is preloaded to avoid issues with regex operations that might try to load it later
- The DynaLoader namespace removal prevents loading of potentially unsafe dynamic extensions
- Cache invalidation (++PL_sub_generation, hv_clear(PL_stashcache)) ensures that security restrictions are properly enforced
- Any errors during initialization result in PostgreSQL ERROR reports with appropriate context information

## Simplified Source

```c
static void
plperl_trusted_init(void)
{
    dTHX;

    // Use original require during setup
    PL_ppaddr[OP_REQUIRE] = pp_require_orig;
    PL_ppaddr[OP_DOFILE] = pp_require_orig;

    // Execute trusted initialization code
    eval_pv(PLC_TRUSTED, FALSE);
    if (SvTRUE(ERRSV))
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                       errmsg("%s", strip_trailing_ws(sv2cstr(ERRSV))),
                       errcontext("while executing PLC_TRUSTED")));

    // Force load utf8 module to prevent runtime errors
    eval_pv("my $a=chr(0x100); return $a =~ /\\xa9/i", FALSE);

    // Lock down the interpreter with security restrictions
    PL_ppaddr[OP_REQUIRE] = pp_require_safe;
    PL_ppaddr[OP_DOFILE] = pp_require_safe;
    PL_op_mask = plperl_opmask;

    // Remove DynaLoader to prevent extension loading
    HV *stash = gv_stashpv("DynaLoader", GV_ADDWARN);
    hv_iterinit(stash);
    SV *sv;
    char *key;
    I32 klen;
    while ((sv = hv_iternextsv(stash, &key, &klen)))
    {
        if (isGV_with_GP(sv) && GvCV(sv))
        {
            SvREFCNT_dec(GvCV(sv));
            GvCV_set(sv, NULL);
        }
    }
    hv_clear(stash);

    // Invalidate caches to enforce security
    ++PL_sub_generation;
    hv_clear(PL_stashcache);

    // Execute user initialization code if configured
    if (plperl_on_plperl_init && *plperl_on_plperl_init)
    {
        eval_pv(plperl_on_plperl_init, FALSE);
        if (SvTRUE(ERRSV))
            ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                           errmsg("%s", strip_trailing_ws(sv2cstr(ERRSV))),
                           errcontext("while executing plperl.on_plperl_init")));
    }
}
```