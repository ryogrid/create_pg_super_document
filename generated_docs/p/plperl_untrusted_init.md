# plperl_untrusted_init

## Location
[src/pl/plperl/plperl.c:1038-1060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L1038-L1060)

## Overview
Initializes the current Perl interpreter as an untrusted interpreter, allowing unrestricted Perl code execution with optional custom initialization.

## Definition

```c
static void
plperl_untrusted_init(void)
```
## Detailed Description
This function configures a Perl interpreter to run in untrusted mode, which provides no security restrictions compared to the trusted mode. The function is intentionally minimal since untrusted interpreters allow full Perl functionality without sandboxing.

The only operation performed is executing user-defined initialization code specified in the plperl.on_plperlu_init configuration parameter. This allows administrators to set up custom environment or load modules for untrusted Perl functions.

Unlike plperl_trusted_init, this function does not:
- Restrict opcodes or operations
- Remove dangerous namespaces like DynaLoader
- Install safe versions of require/dofile
- Set operation masks

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading macro)
  - eval_pv (evaluate Perl code)
  - [strip_trailing_ws](../s/strip_trailing_ws.md)
  - [sv2cstr](../s/sv2cstr.md) (convert Perl scalar to C string)
  - ereport/errcode/errmsg/errcontext (PostgreSQL error reporting)
- Called from:
  - [select_perl_context](../s/select_perl_context.md) (when setting up untrusted Perl context)

## Notes and Other Information
- This function is used for PL/PerlU (untrusted PL/Perl) where security restrictions are not applied
- The minimal implementation reflects that untrusted interpreters have full Perl capabilities
- Custom initialization via plperl.on_plperlu_init allows site-specific setup for untrusted functions
- Any errors during custom initialization result in PostgreSQL ERROR reports
- The function name uses 'plperlu' in the configuration parameter to distinguish from trusted initialization

## Simplified Source

```c
static void
plperl_untrusted_init(void)
{
    dTHX;

    // Execute custom initialization code for untrusted Perl if configured
    if (plperl_on_plperlu_init && *plperl_on_plperlu_init)
    {
        eval_pv(plperl_on_plperlu_init, FALSE);
        if (SvTRUE(ERRSV))
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                     errmsg("%s", strip_trailing_ws(sv2cstr(ERRSV))),
                     errcontext("while executing plperl.on_plperlu_init")));
    }
}
```