# initialize_environment

## Location
[src/test/regress/pg_regress.c:718-922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L718-L922)

## Overview
Prepares and configures environment variables necessary for running PostgreSQL regression tests in a controlled and consistent manner.

## Definition
```c
static void initialize_environment(void)
```

## Detailed Description
The `initialize_environment` function sets up a comprehensive environment configuration for PostgreSQL regression testing. It performs several key tasks: sets essential PostgreSQL-related environment variables (PGAPPNAME, PG_ABS_SRCDIR, etc.), configures locale settings to ensure consistent test results across platforms, handles encoding and timezone settings, manages PostgreSQL connection parameters, and clears potentially interfering environment variables when using a temporary instance.

The function handles two main scenarios: testing with a temporary PostgreSQL instance (temp_instance mode) where it clears all connection-related environment variables and sets up controlled connection parameters, and testing against an existing PostgreSQL installation where it honors existing environment variables but overrides them with command-line options when specified.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- `inputdir`: Source directory for test inputs
- `outputdir`: Build directory for test outputs  
- `dlpath`: Library directory path
- `nolocale`: Flag to clear locale settings
- `encoding`: Client encoding to set
- `temp_instance`: Flag indicating temporary instance usage
- `hostname`: Database host to connect to
- `port`: Database port to connect to
- `user`: Database user for connections

## Dependencies
- Functions called/Symbols referenced:
  - setenv
  - unsetenv
  - [make_temp_sockdir](../m/make_temp_sockdir.md)
  - note
  - [load_resultmap](../l/load_resultmap.md)
  - DEFAULT_PGSOCKET_DIR
  - __darwin__ (preprocessor macro)
  - ENABLE_SSPI (preprocessor macro)
- Called from (representative examples):
  - [regression_main](../r/regression_main.md)

## Notes and Other Information
- Sets timezone to "America/Los_Angeles" and datestyle to "Postgres, MDY" for consistent datetime testing
- Uses PGOPTIONS to set intervalstyle=postgres_verbose while preserving existing options
- Platform-specific locale handling for Windows, Cygwin, and macOS
- Clears LC_MESSAGES and sets to "C" to ensure English error messages for consistent test diffs
- Synchronizes environment clearing with PostgreSQL/Test/Utils.pm for consistency
- Creates temporary socket directory when needed for Unix socket connections
- Part of the PostgreSQL regression testing framework (pg_regress)

## Simplified Source

```c
static void initialize_environment(void) {
    // Set basic PostgreSQL environment variables
    setenv("PGAPPNAME", "pg_regress", 1);
    setenv("PG_ABS_SRCDIR", inputdir, 1);
    setenv("PG_ABS_BUILDDIR", outputdir, 1);
    setenv("PG_LIBDIR", dlpath, 1);
    setenv("PG_DLSUFFIX", DLSUFFIX, 1);

    // Clear locale settings if requested
    if (nolocale) {
        unsetenv("LC_COLLATE");
        unsetenv("LC_CTYPE");
        unsetenv("LC_MONETARY");
        unsetenv("LC_NUMERIC");
        unsetenv("LC_TIME");
        unsetenv("LANG");

        // Set LANG=C on platforms that need it
#if defined(WIN32) || defined(__CYGWIN__) || defined(__darwin__)
        setenv("LANG", "C", 1);
#endif
    }

    // Force English messages for consistent test output
    unsetenv("LANGUAGE");
    unsetenv("LC_ALL");
    setenv("LC_MESSAGES", "C", 1);

    // Set client encoding if specified
    if (encoding)
        setenv("PGCLIENTENCODING", encoding, 1);
    else
        unsetenv("PGCLIENTENCODING");

    // Set consistent timezone and date format
    setenv("PGTZ", "America/Los_Angeles", 1);
    setenv("PGDATESTYLE", "Postgres, MDY", 1);

    // Set intervalstyle while preserving existing PGOPTIONS
    const char *my_pgoptions = "-c intervalstyle=postgres_verbose";
    const char *old_pgoptions = getenv("PGOPTIONS");
    if (!old_pgoptions)
        old_pgoptions = "";
    char *new_pgoptions = psprintf("%s %s", old_pgoptions, my_pgoptions);
    setenv("PGOPTIONS", new_pgoptions, 1);
    free(new_pgoptions);

    if (temp_instance) {
        // Clear all PostgreSQL connection variables for temp instance
        unsetenv("PGCHANNELBINDING");
        unsetenv("PGCONNECT_TIMEOUT");
        unsetenv("PGDATA");
        unsetenv("PGDATABASE");
        unsetenv("PGUSER");
        // ... (many more unsetenv calls)

        // Set connection parameters for temp instance
        if (hostname != NULL)
            setenv("PGHOST", hostname, 1);
        else {
            sockdir = getenv("PG_REGRESS_SOCK_DIR");
            if (!sockdir)
                sockdir = make_temp_sockdir();
            setenv("PGHOST", sockdir, 1);
        }
        unsetenv("PGHOSTADDR");
        if (port != -1) {
            char s[16];
            snprintf(s, sizeof(s), "%d", port);
            setenv("PGPORT", s, 1);
        }
    } else {
        // Honor existing environment, override with command-line options
        if (hostname != NULL) {
            setenv("PGHOST", hostname, 1);
            unsetenv("PGHOSTADDR");
        }
        if (port != -1) {
            char s[16];
            snprintf(s, sizeof(s), "%d", port);
            setenv("PGPORT", s, 1);
        }
        if (user != NULL)
            setenv("PGUSER", user, 1);

        unsetenv("PGDATABASE");  // Never inherit database name

        // Report connection info
        const char *pghost = getenv("PGHOST");
        const char *pgport = getenv("PGPORT");
        if (!pghost && DEFAULT_PGSOCKET_DIR[0] == '\0')
            pghost = "localhost";

        if (pghost && pgport)
            note("using postmaster on %s, port %s", pghost, pgport);
        else if (pghost && !pgport)
            note("using postmaster on %s, default port", pghost);
        else if (!pghost && pgport)
            note("using postmaster on Unix socket, port %s", pgport);
        else
            note("using postmaster on Unix socket, default port");
    }

    load_resultmap();
}
```