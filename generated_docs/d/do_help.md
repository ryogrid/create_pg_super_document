# do_help

## Location
[src/bin/pg_ctl/pg_ctl.c:1961-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1961-L2034)

## Overview
A static utility function in pg_ctl that displays comprehensive help information for all pg_ctl commands and options, providing users with usage instructions and documentation.

## Definition


## Detailed Description
The  function prints detailed usage information for the pg_ctl utility to stdout. It provides a comprehensive overview of all available commands (init, start, stop, restart, reload, status, promote, logrotate, kill, and on Windows: register/unregister), their syntax, and available options. The function uses internationalization support through the  macro to provide localized help text.

The help output is organized into several sections:
- Command usage syntax for each operation
- Common options applicable to multiple commands
- Specific options for start/restart operations
- Options for stop/restart operations
- Explanation of shutdown modes (smart, fast, immediate)
- Allowed signal names for the kill command
- Windows-specific service registration options
- Bug reporting and project information

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - printf (for output formatting)
  - _ (internationalization macro)
  - progname (global variable for program name)
  - HAVE_GETRLIMIT (conditional compilation macro)
  - PACKAGE_BUGREPORT, PACKAGE_NAME, PACKAGE_URL (build-time constants)

- Called from (representative examples):
  - [main](../m/main.md) (when --help option is specified)
  - [write_stderr](../w/write_stderr.md) (indirectly through error handling)

## Notes and Other Information
- The function includes conditional compilation blocks for Windows-specific features (#ifdef WIN32)
- Core file options are platform-dependent and show different messages based on HAVE_GETRLIMIT availability  
- All output text is internationalized using gettext macros for localization support
- The function terminates the program after displaying help information
- Located in src/bin/pg_ctl/pg_ctl.c:1961-2034