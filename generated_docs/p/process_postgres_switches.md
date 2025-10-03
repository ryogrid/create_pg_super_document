# process_postgres_switches

## Location
[src/backend/tcop/postgres.c:3877-4128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3877-L4128)

## Overview
A comprehensive command-line argument parser for PostgreSQL backend processes that handles both secure and insecure configuration options coming from various sources.

## Definition

```c
void
process_postgres_switches(int argc, char *argv[], GucContext ctx,
						  const char **dbname)
```
## Detailed Description
This function is PostgreSQL's primary command-line argument processor for backend processes. It is called twice during server startup: once for "secure" options that come from the postmaster or command line (with PGC_POSTMASTER context), and once for "insecure" options that come from the client's startup packet (with PGC_BACKEND or PGC_SU_BACKEND context).

The function processes a wide range of command-line switches that control various aspects of PostgreSQL's behavior including:
- Memory settings (shared_buffers, work_mem)
- Connection settings (listen_addresses, port, unix_socket_directories)
- Debugging and statistics options
- Query execution options
- Binary upgrade mode
- SSL configuration
- Output redirection

The function uses getopt() for parsing and applies configuration changes through the GUC (Grand Unified Configuration) system using SetConfigOption(). It includes comprehensive error handling and validation, ensuring that insecure options from clients cannot compromise server security.

## Parameters / Member Variables
- `argc`: Number of command-line arguments
- `argv[]`: Array of command-line argument strings, where argv[0] is ignored (assumed to be program name)
- `ctx`: GUC context indicating the source and security level of the options (PGC_POSTMASTER for secure, PGC_BACKEND/PGC_SU_BACKEND for insecure)
- `**dbname`: Pointer to database name string; if initially NULL and a database name is present in arguments, it will be set to the database name
## Dependencies
- Functions called/Symbols referenced:
  - [SetConfigOption](../S/SetConfigOption.md) (for applying configuration changes)
  - [ParseLongOption](../P/ParseLongOption.md) (for parsing --name=value format options)
  - [get_stats_option_name](../g/get_stats_option_name.md) (for mapping statistics option names)
  - [set_debug_options](../s/set_debug_options.md) (for debug level configuration)
  - [set_plan_disabling_options](../s/set_plan_disabling_options.md) (for planner option configuration)
  - [getopt](../g/getopt.md) (for command-line parsing)
  - [strlcpy](../s/strlcpy.md) (for safe string copying)
- Called from (representative examples):
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md) (in src/backend/tcop/postgres.c:4147)
  - [process_startup_options](process_startup_options.md) (in src/backend/utils/init/postinit.c:1297)

## Notes and Other Information
- Must be kept in sync with postmaster/postmaster.c option sets to avoid conflicts
- Handles both short options (-x) and long options (--name=value)
- Some options are restricted to secure contexts only (e.g., binary upgrade mode, output redirection)
- Resets getopt() state after processing to allow multiple calls or use in subprocesses
- Uses different error message formats depending on whether running under postmaster
- The function supports processing database name as a positional argument
- Critical for PostgreSQL's security model by distinguishing between trusted and untrusted option sources

## Simplified Source

```c
// Simplified version of process_postgres_switches
void process_postgres_switches(int argc, char *argv[], GucContext ctx,
                               const char **dbname) {
    bool secure = (ctx == PGC_POSTMASTER);
    int errs = 0;
    GucSource gucsource = secure ? PGC_S_ARGV : PGC_S_CLIENT;
    int flag;

    // Handle --single argument if present in secure mode
    if (secure && argc > 1 && strcmp(argv[1], "--single") == 0) {
        argv++;
        argc--;
    }

    // Process command-line options using getopt
    while ((flag = getopt(argc, argv, "B:bC:c:D:d:EeFf:h:ijk:lN:nOPp:r:S:sTt:v:W:-:")) != -1) {
        switch (flag) {
            case 'B': // shared_buffers
                SetConfigOption("shared_buffers", optarg, ctx, gucsource);
                break;
            case 'b': // binary upgrade mode (secure only)
                if (secure) IsBinaryUpgrade = true;
                break;
            case 'c': // configuration parameter
            case '-': // long option format
                parse_and_set_config_option(optarg, flag, ctx, gucsource);
                break;
            case 'D': // data directory (secure only)
                if (secure) userDoption = strdup(optarg);
                break;
            case 'd': // debug level
                set_debug_options(atoi(optarg), ctx, gucsource);
                break;
            case 'p': // port
                SetConfigOption("port", optarg, ctx, gucsource);
                break;
            case 'h': // listen_addresses
                SetConfigOption("listen_addresses", optarg, ctx, gucsource);
                break;
            // ... many more options processed similarly
            default:
                errs++;
                break;
        }
        if (errs) break;
    }

    // Extract optional database name
    if (!errs && dbname && *dbname == NULL && argc - optind >= 1)
        *dbname = strdup(argv[optind++]);

    // Error handling
    if (errs || argc != optind) {
        report_command_line_error(argv[optind]);
    }

    // Reset getopt state for future use
    optind = 1;
#ifdef HAVE_INT_OPTRESET
    optreset = 1;
#endif
}
```

Key simplifications made:
- Condensed the massive switch statement to show the pattern
- Abstracted repetitive SetConfigOption calls
- Simplified error handling logic
- Focused on the secure vs insecure context handling
- Emphasized the getopt parsing structure
- Removed platform-specific getopt details for clarity