# pgwin32_CommandLine

## Location
[src/bin/pg_ctl/pg_ctl.c:1417-1500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1417-L1500)

## Overview
Constructs a properly formatted command line string for PostgreSQL processes on Windows, handling both service registration and runtime execution scenarios.

## Definition

```c
static char *
pgwin32_CommandLine(bool registration)
```
## Detailed Description
This function builds a complete command line string for executing PostgreSQL processes on Windows. It handles two distinct modes:

1. **Registration Mode** (): Creates a command line for Windows service registration, including the current pg_ctl executable path with 'runservice' command
2. **Runtime Mode** (): Creates a command line for direct postgres executable execution

Key functionality includes:
- **Executable Location**: Uses  for pg_ctl or  for postgres
- **Path Processing**: Ensures .exe extension and converts to Windows native path format with backslashes
- **Argument Assembly**: Constructs command line with proper quoting and Windows-specific formatting
- **Configuration Handling**: Includes data directory (-D), event source (-e), timing options (-t, -w), and additional postgres options (-o)
- **Memory Management**: Uses PQExpBuffer for dynamic string construction

The function carefully handles Windows path conventions and service-specific requirements.

## Parameters / Member Variables
- `registration`: Boolean flag indicating whether to build command line for service registration (true) or direct execution (false)
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [find_my_exec](../f/find_my_exec.md)
  - [find_other_exec](../f/find_other_exec.md)
  - [write_stderr](../w/write_stderr.md)
  - [make_native_path](../m/make_native_path.md)
  - [make_absolute_path](../m/make_absolute_path.md)
  - [pg_strcasecmp](pg_strcasecmp.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - PG_BACKEND_VERSIONSTR
  - DEFAULT_WAIT
- Called from (representative examples):
  - [pgwin32_doRegister](pgwin32_doRegister.md) (src/bin/pg_ctl/pg_ctl.c:1521)
  - [pgwin32_ServiceMain](pgwin32_ServiceMain.md) (src/bin/pg_ctl/pg_ctl.c:1640)

## Notes and Other Information
- The function is static and Windows-specific, only used within pg_ctl.c
- Returns dynamically allocated string (PQExpBuffer->data) that caller is responsible for freeing
- Automatically appends .exe extension if not present in the executable path
- Uses double-quoted paths throughout to handle Windows paths with spaces
- Includes comprehensive option handling for PostgreSQL service configuration
- Distinguishes between service registration options (like -N for service name) and runtime options
- Proper error handling with program termination on critical failures
- Part of pg_ctl's Windows service management infrastructure

## Simplified Source

```c
static char *
pgwin32_CommandLine(bool registration)
{
    PQExpBuffer cmdLine = createPQExpBuffer();
    char cmdPath[MAXPGPATH];
    int ret;

    // Find the appropriate executable
    if (registration) {
        // For service registration, use pg_ctl executable
        ret = find_my_exec(argv0, cmdPath);
        if (ret != 0) {
            write_stderr("Could not find own program executable\n");
            exit(1);
        }
    } else {
        // For runtime, use postgres executable
        ret = find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR, cmdPath);
        if (ret != 0) {
            write_stderr("Could not find postgres program executable\n");
            exit(1);
        }
    }

    // Ensure .exe extension for Windows
    if (strlen(cmdPath) < 4 ||
        pg_strcasecmp(cmdPath + strlen(cmdPath) - 4, ".exe") != 0) {
        snprintf(cmdPath + strlen(cmdPath), sizeof(cmdPath) - strlen(cmdPath), ".exe");
    }

    // Convert to native Windows path format
    make_native_path(cmdPath);

    // Build command line with quoted executable path
    appendPQExpBuffer(cmdLine, "\"%s\"", cmdPath);

    // Add service-specific options for registration
    if (registration)
        appendPQExpBuffer(cmdLine, " runservice -N \"%s\"", register_servicename);

    // Add data directory if specified
    if (pg_config) {
        char *dataDir = make_absolute_path(pg_config);
        if (dataDir == NULL)
            exit(1);
        make_native_path(dataDir);
        appendPQExpBuffer(cmdLine, " -D \"%s\"", dataDir);
        free(dataDir);
    }

    // Add additional options based on configuration
    if (registration && event_source != NULL)
        appendPQExpBuffer(cmdLine, " -e \"%s\"", event_source);

    if (registration && do_wait)
        appendPQExpBufferStr(cmdLine, " -w");

    if (registration && wait_seconds_arg && wait_seconds != DEFAULT_WAIT)
        appendPQExpBuffer(cmdLine, " -t %d", wait_seconds);

    if (registration && silent_mode)
        appendPQExpBufferStr(cmdLine, " -s");

    // Add postgres-specific options
    if (post_opts) {
        if (registration)
            appendPQExpBuffer(cmdLine, " -o \"%s\"", post_opts);
        else
            appendPQExpBuffer(cmdLine, " %s", post_opts);
    }

    return cmdLine->data;
}
```