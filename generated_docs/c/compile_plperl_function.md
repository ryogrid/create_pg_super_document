# compile_plperl_function

## Location
src/pl/plperl/plperl.c: 2718 - 2997

## Overview
Compiles or retrieves a cached PL/Perl function descriptor, handling the complete process from cache lookup to Perl code compilation and storage.

## Definition


## Detailed Description
This is a comprehensive function that manages the entire lifecycle of PL/Perl function compilation. It first attempts to find an existing cached function descriptor in the hash table, validating it against the current pg_proc entry. If no valid cached version exists, it creates a new function descriptor by analyzing the function's metadata, setting up memory contexts, processing argument and return types, extracting the source code, and compiling it in the appropriate Perl interpreter. The function handles both trusted (plperl) and untrusted (plperlu) variants, different function types (regular, trigger, event trigger), and includes comprehensive error handling with proper cleanup.

## Parameters / Member Variables
- : Object ID of the function to compile
- : Boolean indicating if this is a trigger function
- : Boolean indicating if this is an event trigger function

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md): Retrieves pg_proc and related tuples
  - [hash_search](../h/hash_search.md): Searches and manages procedure hash table
  - [validate_plperl_function](../v/validate_plperl_function.md): Validates cached function descriptors
  - AllocSetContextCreate: Creates memory context for function data
  - [plperl_compile_callback](../p/plperl_compile_callback.md): Error callback for compilation errors
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)/SysCacheGetAttrNotNull: Extracts procedure attributes
  - [oid_array_to_list](../o/oid_array_to_list.md): Converts transform types array
  - [type_is_rowtype](../t/type_is_rowtype.md)/IsTrueArrayType: Type analysis functions
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md): Sets up function manager info
  - [getTypeIOParam](../g/getTypeIOParam.md): Gets type I/O parameters
  - TextDatumGetCString: Extracts function source code
  - [select_perl_context](../s/select_perl_context.md): Selects trusted/untrusted Perl context
  - [plperl_create_sub](../p/plperl_create_sub.md): Compiles Perl subroutine
  - [activate_interpreter](../a/activate_interpreter.md): Manages Perl interpreter state
  - increment_prodesc_refcount: Manages reference counting
  - [free_plperl_function](../f/free_plperl_function.md): Cleanup function for error cases
- Called from:
  - [plperl_validator](../p/plperl_validator.md): During function validation
  - [plperl_func_handler](../p/plperl_func_handler.md): For regular function execution
  - [plperl_trigger_handler](../p/plperl_trigger_handler.md): For trigger function execution
  - [plperl_event_trigger_handler](../p/plperl_event_trigger_handler.md): For event trigger execution

## Notes and Other Information
- Implements a two-tier caching strategy for plperl and plperlu functions
- Handles CREATE OR REPLACE FUNCTION by validating cached descriptors
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH/PG_END_TRY)
- Supports function polymorphism and transform types
- Validates argument and return types, rejecting inappropriate pseudotypes
- Manages memory contexts to prevent leaks during compilation errors
- Located at src/pl/plperl/plperl.c:2718-2997
- Critical function in PL/Perl's function management infrastructure