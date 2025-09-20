# LoadArchiveLibrary

## Location
[src/backend/postmaster/pgarch.c:911-952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L911-L952)

## Overview
Loads the archiving callbacks into the local ArchiveCallbacks global variable, initializing either shell-based archiving or external library-based archiving based on configuration.

## Definition

```c
static void
LoadArchiveLibrary(void)
```
## Detailed Description
LoadArchiveLibrary is responsible for initializing PostgreSQL's archiving mechanism by loading the appropriate archiving callbacks. The function supports two modes of operation:

1. **Shell-based archiving**: When  is not set, it uses the built-in shell archiving functionality via .
2. **External library archiving**: When  is configured, it dynamically loads an external library and calls its  function.

The function performs validation to ensure only one archiving method is configured, initializes the archive module state, calls any startup callbacks, and registers a shutdown callback to be executed on process exit.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [shell_archive_init](../s/shell_archive_init.md)
  - [load_external_function](../l/load_external_function.md)
  - [ArchiveModuleState](../A/ArchiveModuleState.md)
  - [pgarch_call_module_shutdown_cb](../p/pgarch_call_module_shutdown_cb.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
- Called from (representative examples):
  - [PgArchiverMain](../P/PgArchiverMain.md)
  - [arch_files_state](../a/arch_files_state.md) (indirectly)

## Notes and Other Information
- The function enforces mutual exclusivity between  and  configuration parameters
- Allocates memory for  using  to ensure zero-initialized state
- Registers  as a shutdown callback via 
- The external library must export a  symbol
- The archive module must provide at least an  callback function
- Located in src/backend/postmaster/pgarch.c:911-952