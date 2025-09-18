# process_shared_preload_libraries

## Location
src/backend/utils/init/miscinit.c: 1898 - 1911

## Overview
process_shared_preload_libraries loads shared libraries specified in the shared_preload_libraries configuration parameter during PostgreSQL server startup.

## Definition
```c
void process_shared_preload_libraries(void)
```

## Detailed Description
This function is responsible for loading shared libraries that have been configured to be preloaded when the PostgreSQL server starts. It processes the shared_preload_libraries GUC parameter, which contains a comma-separated list of library names that should be loaded into the postmaster process before it forks child processes.

The function sets progress tracking flags to indicate that shared preload library processing is in progress and completed. This allows other parts of the system to know the current state of library loading. Unlike session preload libraries, shared preload libraries are loaded without path restrictions (restricted=false), meaning they can be loaded from any location accessible to the PostgreSQL server.

The libraries loaded by this function are available to all PostgreSQL processes since they are loaded in the postmaster before process forking occurs. This makes shared preload libraries ideal for extensions that need to be available system-wide or that require initialization at the postmaster level.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [load_libraries](../l/load_libraries.md)
  - process_shared_preload_libraries_in_progress (global variable)
  - process_shared_preload_libraries_done (global variable)  
  - shared_preload_libraries_string (global variable)
- Called from (representative examples):
  - [SubPostmasterMain](../S/SubPostmasterMain.md)
  - [PostmasterMain](../P/PostmasterMain.md)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md)
  - INIT_PG_OVERRIDE_ROLE_LOGIN

## Notes and Other Information
- This function is called during server startup, specifically in the postmaster process
- Libraries loaded here are inherited by all child processes due to the fork model
- Uses unrestricted loading (restricted=false) allowing libraries from any accessible path
- The progress tracking variables help coordinate with other subsystems that may depend on these libraries
- Shared preload libraries are typically used for monitoring tools, custom background workers, and system-wide extensions
- Loading failures are handled by the underlying load_libraries function
- This is different from process_session_preload_libraries which loads libraries per-session with security restrictions