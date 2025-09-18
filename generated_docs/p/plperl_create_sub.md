# plperl_create_sub

## Location
src/pl/plperl/plperl.c: 2095 - 2167

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