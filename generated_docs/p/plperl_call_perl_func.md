# plperl_call_perl_func

## Location
src/pl/plperl/plperl.c: 2180 - 2272

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
  - get_func_signature
  - plperl_hash_from_datum
  - plperl_ref_from_pg_array
  - get_transform_fromsql
  - OidFunctionCall1
  - OutputFunctionCall
  - cstr2sv
  - call_sv
  - strip_trailing_ws
  - sv2cstr
- Called from:
  - plperl_inline_handler
  - plperl_func_handler

## Notes and Other Information
- Uses Perl XS macros (dTHX, dSP, ENTER, SAVETMPS, etc.) for proper Perl API integration
- Handles three main argument conversion paths: NULL values, row types (converted to hash), and scalar types
- Supports PostgreSQL transform functions for custom type conversions
- Uses G_SCALAR | G_EVAL flags when calling Perl subroutine to ensure scalar context and error handling
- Performs comprehensive error checking including return count validation and Perl error (ERRSV) handling
- Memory management follows Perl conventions with proper mortal SV handling and cleanup