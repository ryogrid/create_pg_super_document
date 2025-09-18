# process_postgres_switches

## Location
src/backend/tcop/postgres.c: 3877 - 4128

## Overview
A comprehensive command-line argument parser for PostgreSQL backend processes that handles both secure and insecure configuration options coming from various sources.

## Definition


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
- : Number of command-line arguments
- : Array of command-line argument strings, where argv[0] is ignored (assumed to be program name)
- : GUC context indicating the source and security level of the options (PGC_POSTMASTER for secure, PGC_BACKEND/PGC_SU_BACKEND for insecure)
- : Pointer to database name string; if initially NULL and a database name is present in arguments, it will be set to the database name

## Dependencies
- Functions called/Symbols referenced:
  - SetConfigOption (for applying configuration changes)
  - ParseLongOption (for parsing --name=value format options)
  - get_stats_option_name (for mapping statistics option names)
  - set_debug_options (for debug level configuration)
  - set_plan_disabling_options (for planner option configuration)
  - getopt (for command-line parsing)
  - strlcpy (for safe string copying)
- Called from (representative examples):
  - PostgresSingleUserMain (in src/backend/tcop/postgres.c:4147)
  - process_startup_options (in src/backend/utils/init/postinit.c:1297)

## Notes and Other Information
- Must be kept in sync with postmaster/postmaster.c option sets to avoid conflicts
- Handles both short options (-x) and long options (--name=value)
- Some options are restricted to secure contexts only (e.g., binary upgrade mode, output redirection)
- Resets getopt() state after processing to allow multiple calls or use in subprocesses
- Uses different error message formats depending on whether running under postmaster
- The function supports processing database name as a positional argument
- Critical for PostgreSQL's security model by distinguishing between trusted and untrusted option sources