# main

## Location
src/bin/scripts/vacuumdb.c: 97 - 427

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
  - pgwin32_install_crashdump_handler (Windows crash handling)
  - get_progname (extract program name from argv[0])
  - startup_hacks (platform-specific initialization)
  - save_ps_display_args (preserve argv for process title display)
  - MemoryContextInit (initialize memory management)
  - set_pglocale_pgservice (set up localization)
  - init_locale (configure various locale categories)
  - help (display help information)
  - check_root (verify not running as root)
  - BootstrapModeMain (bootstrap/check mode entry point)
  - SubPostmasterMain (subprocess mode entry point)
  - GucInfoMain (configuration description mode)
  - PostgresSingleUserMain (single-user mode entry point)
  - PostmasterMain (normal server mode entry point)
- Called from (representative examples):
  - Entry point - not called by other functions

## Notes and Other Information
- Sets the global variable `reached_main` to true for crash reporting
- Processes must not return from this function - the specialized main functions should not return, and if they do, the process calls abort()
- Platform-specific behavior includes Windows crash dump handler installation
- Locale handling is carefully orchestrated to support both postmaster and backend processes
- Root privilege checking can be bypassed for safe read-only operations like --describe-config and -C