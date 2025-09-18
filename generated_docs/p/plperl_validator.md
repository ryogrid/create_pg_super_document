# plperl_validator

## Location
src/pl/plperl/plperl.c: 1989 - 2066

## Overview
Validates PL/Perl function definitions during CREATE FUNCTION, checking argument and return types, and optionally compiling the function body for syntax errors.

## Definition


## Detailed Description
This function performs validation of PL/Perl functions when they are created or modified using CREATE FUNCTION or ALTER FUNCTION statements. It examines the function's metadata in pg_proc to validate that the function's signature is compatible with PL/Perl restrictions. The validator checks that return types and argument types are supported by PL/Perl, specifically disallowing most pseudotypes except for triggers, event triggers, records, and void. When check_function_bodies is enabled, it also compiles the function body to detect syntax errors early during function creation rather than at runtime.

## Parameters / Member Variables
- Implicit  parameter (accessed via PG_GETARG_OID): Object ID of the function being validated

## Dependencies
- Functions called/Symbols referenced:
  - [CheckFunctionValidatorAccess](../C/CheckFunctionValidatorAccess.md) (security check for validator access)
  - [SearchSysCache1](../S/SearchSysCache1.md) (look up function in pg_proc catalog)
  - HeapTupleIsValid (validate tuple from catalog lookup)
  - Form_pg_proc (pg_proc tuple structure)
  - [get_typtype](../g/get_typtype.md) (get PostgreSQL type category)
  - TYPTYPE_PSEUDO (pseudotype category constant)
  - TRIGGEROID, EVENT_TRIGGEROID, RECORDOID, VOIDOID (type OID constants)
  - [get_func_arg_info](../g/get_func_arg_info.md) (extract function argument information)
  - [format_type_be](../f/format_type_be.md) (format type name for error messages)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (release catalog cache entry)
  - [compile_plperl_function](../c/compile_plperl_function.md) (compile function body for validation)
  - PG_RETURN_VOID (return void result)
- Called from (representative examples):
  - [plperlu_validator](plperlu_validator.md)

## Notes and Other Information
- Validates function signatures during CREATE FUNCTION and ALTER FUNCTION
- Prevents creation of functions with unsupported PostgreSQL data types
- Allows trigger functions (return type TRIGGER) and event trigger functions (return type EVENT_TRIGGER)
- Allows RECORD and VOID return types but disallows other pseudotypes
- Blocks pseudotype arguments except for RECORD type
- Performs optional function body compilation when check_function_bodies GUC is enabled
- Uses PostgreSQL's system cache to look up function metadata efficiently
- Essential for preventing runtime errors by catching type mismatches at function definition time
- Returns void as validators don't produce meaningful return values