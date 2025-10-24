# do_help

## Location
[src/bin/pg_ctl/pg_ctl.c:1961-2034](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1961-L2034)

## Overview
A static utility function in pg_ctl that displays comprehensive help information for all pg_ctl commands and options, providing users with usage instructions and documentation.

## Definition

```c
static void
do_help(void)
```
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

## Simplified Source

```c
static void
do_help(void)
{
    // Print main usage description
    printf(_("%s is a utility to initialize, start, stop, or control a PostgreSQL server.\n\n"), progname);

    // Display command syntax for all operations
    printf(_("Usage:\n"));
    printf(_("  %s init[db]   [-D DATADIR] [-s] [-o OPTIONS]\n"), progname);
    printf(_("  %s start      [-D DATADIR] [-l FILENAME] [-W] [-t SECS] [-s] [-o OPTIONS] [-p PATH] [-c]\n"), progname);
    printf(_("  %s stop       [-D DATADIR] [-m SHUTDOWN-MODE] [-W] [-t SECS] [-s]\n"), progname);
    printf(_("  %s restart    [-D DATADIR] [-m SHUTDOWN-MODE] [-W] [-t SECS] [-s] [-o OPTIONS] [-c]\n"), progname);
    printf(_("  %s reload     [-D DATADIR] [-s]\n"), progname);
    printf(_("  %s status     [-D DATADIR]\n"), progname);
    printf(_("  %s promote    [-D DATADIR] [-W] [-t SECS] [-s]\n"), progname);
    printf(_("  %s logrotate  [-D DATADIR] [-s]\n"), progname);
    printf(_("  %s kill       SIGNALNAME PID\n"), progname);

#ifdef WIN32
    // Windows service commands
    printf(_("  %s register   [-D DATADIR] [-N SERVICENAME] [-U USERNAME] [-P PASSWORD] [-S START-TYPE] [-e SOURCE] [-W] [-t SECS] [-s] [-o OPTIONS]\n"), progname);
    printf(_("  %s unregister [-N SERVICENAME]\n"), progname);
#endif

    // Common options section
    printf(_("\nCommon options:\n"));
    printf(_("  -D, --pgdata=DATADIR   location of the database storage area\n"));
    printf(_("  -s, --silent           only print errors, no informational messages\n"));
    printf(_("  -t, --timeout=SECS     seconds to wait when using -w option\n"));
    printf(_("  -V, --version          output version information, then exit\n"));
    printf(_("  -w, --wait             wait until operation completes (default)\n"));
    printf(_("  -W, --no-wait          do not wait until operation completes\n"));
    printf(_("  -?, --help             show this help, then exit\n"));

    // Start/restart specific options
    printf(_("\nOptions for start or restart:\n"));
    printf(_("  -c, --core-files       allow postgres to produce core files\n"));
    printf(_("  -l, --log=FILENAME     write (or append) server log to FILENAME\n"));
    printf(_("  -o, --options=OPTIONS  command line options to pass to postgres or initdb\n"));
    printf(_("  -p PATH-TO-POSTGRES    normally not necessary\n"));

    // Stop/restart options and shutdown modes
    printf(_("\nOptions for stop or restart:\n"));
    printf(_("  -m, --mode=MODE        MODE can be \"smart\", \"fast\", or \"immediate\"\n"));

    printf(_("\nShutdown modes are:\n"));
    printf(_("  smart       quit after all clients have disconnected\n"));
    printf(_("  fast        quit directly, with proper shutdown (default)\n"));
    printf(_("  immediate   quit without complete shutdown; will lead to recovery on restart\n"));

    // Additional sections (kill signals, Windows options, bug reports)
    printf(_("\nAllowed signal names for kill:\n"));
    printf("  ABRT HUP INT KILL QUIT TERM USR1 USR2\n");

#ifdef WIN32
    printf(_("\nOptions for register and unregister:\n"));
    printf(_("  -N SERVICENAME  service name with which to register PostgreSQL server\n"));
    printf(_("  -P PASSWORD     password of account to register PostgreSQL server\n"));
    printf(_("  -U USERNAME     user name of account to register PostgreSQL server\n"));
    printf(_("  -S START-TYPE   service start type to register PostgreSQL server\n"));

    printf(_("\nStart types are:\n"));
    printf(_("  auto       start service automatically during system startup (default)\n"));
    printf(_("  demand     start service on demand\n"));
#endif

    // Footer information
    printf(_("\nReport bugs to <%s>.\n"), PACKAGE_BUGREPORT);
    printf(_("%s home page: <%s>\n"), PACKAGE_NAME, PACKAGE_URL);
}
```