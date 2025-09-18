# load_external_function

## Location
src/backend/utils/fmgr/dfmgr.c: 105 - 143

## Overview
This function loads a specified dynamic-link library file and looks for a named function within it, providing the core mechanism for PostgreSQL's dynamic function loading capabilities.

## Definition


## Detailed Description
The  function is a key component of PostgreSQL's dynamic function manager (dfmgr) that enables loading external C functions from shared libraries at runtime. It first expands the possibly-abbreviated filename to an exact path name, then loads the shared library using the internal loading mechanism. Once the library is loaded, it searches for the specified function name within the library using the system's dynamic symbol lookup functionality.

The function provides flexibility in error handling - it can either raise an error when a function is not found or return NULL based on the  parameter. Additionally, it can return a handle to the loaded library for efficient subsequent function lookups from the same library.

## Parameters / Member Variables
- : The name or path of the dynamic library file to load (may be abbreviated)
- : The name of the function to look up within the loaded library
- : Boolean flag indicating whether to raise an error (true) or return NULL (false) when the function is not found
- : Optional output parameter that receives a handle to the loaded library for subsequent use

## Dependencies
- Functions called/Symbols referenced:
  - expand_dynamic_library_name
  - internal_load_library
  - dlsym
- Called from (representative examples):
  - LookupParallelWorkerFunction
  - fmgr_c_validator
  - provider_init
  - llvm_resolve_symbol
  - fmgr_info_C_lang

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- Library loading errors will always trigger ereport() regardless of the signalNotFound setting
- The returned filehandle can be used with lookup_external_function for more efficient subsequent function lookups from the same library
- Memory allocated for the expanded filename is properly cleaned up with pfree()
- The function uses the standard POSIX dlsym() function for symbol lookup within the loaded library