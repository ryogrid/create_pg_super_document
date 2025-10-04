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

## Simplified Source
```c
int main(int argc, char *argv[]) {
    bool do_check_root = true;

    // Mark that main has been reached for crash reporting
    reached_main = true;

    // Install crash handler on Windows
#if defined(WIN32)
    pgwin32_install_crashdump_handler();
#endif

    // Get program name and perform platform-specific startup
    progname = get_progname(argv[0]);
    startup_hacks(progname);

    // Preserve argv for process display and initialize core subsystems
    argv = save_ps_display_args(argc, argv);
    MyProcPid = getpid();
    MemoryContextInit();

    // Set up locale information for internationalization
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("postgres"));
    init_locale("LC_COLLATE", LC_COLLATE, "");
    init_locale("LC_CTYPE", LC_CTYPE, "");
    init_locale("LC_MESSAGES", LC_MESSAGES, "");

    // Keep these locales fixed to "C" for consistency
    init_locale("LC_MONETARY", LC_MONETARY, "C");
    init_locale("LC_NUMERIC", LC_NUMERIC, "C");
    init_locale("LC_TIME", LC_TIME, "C");
    unsetenv("LC_ALL");

    // Handle standard command-line options
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(progname);
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            fputs(PG_BACKEND_VERSIONSTR, stdout);
            exit(0);
        }

        // Allow certain read-only operations to bypass root check
        if (strcmp(argv[1], "--describe-config") == 0)
            do_check_root = false;
        else if (argc > 2 && strcmp(argv[1], "-C") == 0)
            do_check_root = false;
    }

    // Ensure not running as root unless safe operation
    if (do_check_root)
        check_root(progname);

    // Dispatch to appropriate subprogram based on arguments
    if (argc > 1 && strcmp(argv[1], "--check") == 0)
        BootstrapModeMain(argc, argv, true);
    else if (argc > 1 && strcmp(argv[1], "--boot") == 0)
        BootstrapModeMain(argc, argv, false);
#ifdef EXEC_BACKEND
    else if (argc > 1 && strncmp(argv[1], "--forkchild", 11) == 0)
        SubPostmasterMain(argc, argv);
#endif
    else if (argc > 1 && strcmp(argv[1], "--describe-config") == 0)
        GucInfoMain();
    else if (argc > 1 && strcmp(argv[1], "--single") == 0)
        PostgresSingleUserMain(argc, argv, strdup(get_user_name_or_exit(progname)));
    else
        PostmasterMain(argc, argv);

    // Should never reach here - subprograms don't return
    abort();
}
```