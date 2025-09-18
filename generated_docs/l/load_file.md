# load_file

## Location
src/backend/utils/fmgr/dfmgr.c: 144 - 165

## Overview
This function loads a shared library file without looking up any particular function in it, with optional security restrictions for plugin directory access.

## Definition
```c
void load_file(const char *filename, bool restricted)
```

## Detailed Description
The `load_file` function provides a simplified interface for loading shared libraries when you don't need to immediately look up specific functions within them. It handles the complete loading process including filename expansion and security checks. When the `restricted` parameter is true, it enforces security by only allowing libraries from the presumed-secure $libdir/plugins directory to be loaded. If the same shared library has been previously loaded, it will unload and reload it, ensuring a fresh copy is available.

This function is commonly used in PostgreSQL's subscription and replication systems, as well as for loading utility libraries during server initialization.

## Parameters / Member Variables
- `filename`: The name or path of the shared library file to load (may be abbreviated)
- `restricted`: Boolean flag that when true, restricts loading to only libraries in the secure $libdir/plugins directory

## Dependencies
- Functions called/Symbols referenced:
  - [check_restricted_library_name](../c/check_restricted_library_name.md)
  - [expand_dynamic_library_name](../e/expand_dynamic_library_name.md)
  - [internal_load_library](../i/internal_load_library.md)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)
  - [AlterSubscription](../A/AlterSubscription.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [load_libraries](load_libraries.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- This function is part of PostgreSQL's dynamic function management system located in src/backend/utils/fmgr/dfmgr.c
- The function will unload and reload a library if it has been previously loaded
- Security restriction is enforced through check_restricted_library_name when the restricted parameter is true
- Memory allocated for the expanded filename is properly cleaned up with pfree()
- Commonly used in replication and subscription management operations
- The function does not return any value or handle - it simply ensures the library is loaded into memory