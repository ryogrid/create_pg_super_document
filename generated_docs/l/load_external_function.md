# load_external_function

## Location
[src/backend/utils/fmgr/dfmgr.c:105-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L105-L143)

## Overview
This function loads a specified dynamic-link library file and looks for a named function within it, providing the core mechanism for PostgreSQL's dynamic function loading capabilities.

## Definition

```c
void *
load_external_function(const char *filename, const char *funcname,
					   bool signalNotFound, void **filehandle)
```
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
  - [expand_dynamic_library_name](../e/expand_dynamic_library_name.md)
  - [internal_load_library](../i/internal_load_library.md)
  - [dlsym](../d/dlsym.md)
- Called from (representative examples):
  - [LookupParallelWorkerFunction](../L/LookupParallelWorkerFunction.md)
  - [fmgr_c_validator](../f/fmgr_c_validator.md)
  - [provider_init](../p/provider_init.md)
  - [llvm_resolve_symbol](llvm_resolve_symbol.md)
  - [fmgr_info_C_lang](../f/fmgr_info_C_lang.md)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- Library loading errors will always trigger ereport() regardless of the signalNotFound setting
- The returned filehandle can be used with lookup_external_function for more efficient subsequent function lookups from the same library
- Memory allocated for the expanded filename is properly cleaned up with pfree()
- The function uses the standard POSIX dlsym() function for symbol lookup within the loaded library

## Simplified Source

```c
void *
load_external_function(const char *filename, const char *funcname,
                      bool signalNotFound, void **filehandle)
{
    char *fullname;
    void *lib_handle;
    void *function_ptr;

    // Expand filename to full path
    fullname = expand_dynamic_library_name(filename);

    // Load the shared library
    lib_handle = internal_load_library(fullname);

    // Return library handle if requested
    if (filehandle)
        *filehandle = lib_handle;

    // Look up the function in the library
    function_ptr = dlsym(lib_handle, funcname);

    // Handle function not found case
    if (function_ptr == NULL && signalNotFound) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("could not find function \"%s\" in file \"%s\"",
                        funcname, fullname)));
    }

    // Clean up and return
    pfree(fullname);
    return function_ptr;
}
```