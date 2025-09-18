# main

## Location
[src/bin/scripts/vacuumdb.c:97-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L97-L427)

## Overview
The main entry point for any PostgreSQL server process, responsible for initializing essential subsystems and dispatching to appropriate subprograms based on command-line arguments.

## Definition
```c
int main(int argc, char *argv[])
```

## Detailed Description
The main function serves as the universal entry point for all PostgreSQL server processes. It performs critical initialization tasks including setting up error handling, memory management, locale configuration, and process identification. After handling standard command-line options (--help, --version, --describe-config), it dispatches execution to one of several specialized main functions based on the startup mode:

- Bootstrap mode (--boot/--check) for database initialization
- Subprocess mode (--forkchild) for background processes 
- Single-user mode (--single) for standalone database access
- Normal postmaster mode (default) for the main server daemon

The function ensures that PostgreSQL is not running as root (with exceptions for safe read-only operations) and sets up platform-specific crash handling where supported.

## Parameters / Member Variables
- `argc`: Number of command-line arguments passed to the program
- `argv`: Array of command-line argument strings

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_install_crashdump_handler](../p/pgwin32_install_crashdump_handler.md) (Windows crash handling)
  - [get_progname](../g/get_progname.md) (extract program name from argv[0])
  - [startup_hacks](../s/startup_hacks.md) (platform-specific initialization)
  - [save_ps_display_args](../s/save_ps_display_args.md) (preserve argv for process title display)
  - [MemoryContextInit](../M/MemoryContextInit.md) (initialize memory management)
  - [set_pglocale_pgservice](../s/set_pglocale_pgservice.md) (set up localization)
  - [init_locale](../i/init_locale.md) (configure various locale categories)
  - [help](../h/help.md) (display help information)
  - [check_root](../c/check_root.md) (verify not running as root)
  - [BootstrapModeMain](../B/BootstrapModeMain.md) (bootstrap/check mode entry point)
  - [SubPostmasterMain](../S/SubPostmasterMain.md) (subprocess mode entry point)
  - [GucInfoMain](../G/GucInfoMain.md) (configuration description mode)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md) (single-user mode entry point)
  - [PostmasterMain](../P/PostmasterMain.md) (normal server mode entry point)
- Called from (representative examples):
  - Entry point - not called by other functions

## Notes and Other Information
- Sets the global variable `reached_main` to true for crash reporting
- Processes must not return from this function - the specialized main functions should not return, and if they do, the process calls abort()
- Platform-specific behavior includes Windows crash dump handler installation
- Locale handling is carefully orchestrated to support both postmaster and backend processes
- Root privilege checking can be bypassed for safe read-only operations like --describe-config and -C