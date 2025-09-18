# regression_main

## Location
src/test/regress/pg_regress.c: 2064 - 2565

## Overview
The main entry point function for PostgreSQL's regression testing framework that handles command-line parsing, test environment setup, and coordination of the entire testing process.

## Definition


## Detailed Description
The  function is the core orchestrator of PostgreSQL's regression testing framework. It provides a comprehensive command-line interface for configuring and running regression tests, handles both temporary and existing PostgreSQL instances, and manages the complete lifecycle of test execution.

Key responsibilities include:
- Command-line argument parsing with extensive options support
- Database initialization (via initdb or template copying)
- Temporary PostgreSQL instance creation and management
- Connection parameter setup and server readiness verification
- Test environment configuration and cleanup
- Cross-platform compatibility handling (Unix/Windows socket differences)

The function supports multiple testing modes including single-user mode, bootstrap mode, and various debugging configurations. It can create temporary PostgreSQL instances or work with existing ones, making it flexible for different testing scenarios.

## Parameters / Member Variables
- : Standard argument count from main()
- : Standard argument vector from main() 
- : Initialization function pointer called early to set up test-specific defaults
- : Test start function pointer (for test execution coordination)
- : Post-processing function pointer (for result processing after tests complete)

## Dependencies
- Functions called/Symbols referenced:
  - pg_logging_init, get_progname, set_pglocale_pgservice
  - getopt_long (command-line parsing)
  - help (displays usage information)
  - make_absolute_path, directory_exists, make_directory
  - spawn_process, PQpingParams (PostgreSQL connection testing)
  - bail, note, diag (error handling and logging)
  - initialize_environment, open_result_files
  - config_sspi_auth (Windows SSPI authentication setup)
- Called from (representative examples):
  - main (in pg_regress_main.c, isolation_main.c, pg_regress_ecpg.c)

## Notes and Other Information
- Supports extensive command-line options for database configuration, connection parameters, test selection, and debugging
- Handles cross-platform differences (Unix domain sockets vs TCP on Windows)
- Can work with temporary instances (created via initdb) or existing PostgreSQL installations
- Includes sophisticated port conflict detection and automatic port assignment
- Supports template-based database initialization for faster test setup
- Implements timeout mechanisms for server startup with configurable wait times via PGCTLTIMEOUT
- Includes comprehensive error handling with detailed logging to help diagnose test failures
- Uses function pointers to allow different test frameworks (main regression, isolation tests, ECPG tests) to customize behavior
- Returns integer exit code indicating success/failure status