# regression_main

## Location
[src/test/regress/pg_regress.c:2064-2565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L2064-L2565)

## Overview
The main entry point function for PostgreSQL's regression testing framework that handles command-line parsing, test environment setup, and coordination of the entire testing process.

## Definition

```c
struct option long_options[] = {
		{"help", no_argument, NULL, 'h'},
		{"version", no_argument, NULL, 'V'},
		{"dbname", required_argument, NULL, 1},
		{"debug", no_argument, NULL, 2},
		{"inputdir", required_argument, NULL, 3},
		{"max-connections", required_argument, NULL, 5},
		{"encoding", required_argument, NULL, 6},
		{"outputdir", required_argument, NULL, 7},
		{"schedule", required_argument, NULL, 8},
		{"temp-instance", required_argument, NULL, 9},
		{"no-locale", no_argument, NULL, 10},
		{"host", required_argument, NULL, 13},
		{"port", required_argument, NULL, 14},
		{"user", required_argument, NULL, 15},
		{"bindir", required_argument, NULL, 16},
		{"dlpath", required_argument, NULL, 17},
		{"create-role", required_argument, NULL, 18},
		{"temp-config", required_argument, NULL, 19},
		{"use-existing", no_argument, NULL, 20},
		{"launcher", required_argument, NULL, 21},
		{"load-extension", required_argument, NULL, 22},
		{"config-auth", required_argument, NULL, 24},
		{"max-concurrent-tests", required_argument, NULL, 25},
		{"expecteddir", required_argument, NULL, 26},
		{NULL, 0, NULL, 0}
	};
```
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
  - [pg_logging_init](../p/pg_logging_init.md), get_progname, set_pglocale_pgservice
  - [getopt_long](../g/getopt_long.md) (command-line parsing)
  - [help](../h/help.md) (displays usage information)
  - [make_absolute_path](../m/make_absolute_path.md), directory_exists, make_directory
  - [spawn_process](../s/spawn_process.md), PQpingParams (PostgreSQL connection testing)
  - bail, note, diag (error handling and logging)
  - [initialize_environment](../i/initialize_environment.md), open_result_files
  - [config_sspi_auth](../c/config_sspi_auth.md) (Windows SSPI authentication setup)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_regress_main.c, isolation_main.c, pg_regress_ecpg.c)

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

## Simplified Source

```c
int regression_main(int argc, char *argv[],
                   init_function ifunc,
                   test_start_function startfunc,
                   postprocess_result_function postfunc)
{
    // Command-line option definitions
    static struct option long_options[] = {
        {"help", no_argument, NULL, 'h'},
        {"version", no_argument, NULL, 'V'},
        {"dbname", required_argument, NULL, 1},
        {"debug", no_argument, NULL, 2},
        {"inputdir", required_argument, NULL, 3},
        // ... additional options for connections, directories, etc.
        {NULL, 0, NULL, 0}
    };

    // Initialize logging and program setup
    pg_logging_init(argv[0]);
    progname = get_progname(argv[0]);
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_regress"));

    // Platform-specific socket configuration
    #if defined(WIN32)
    use_unix_sockets = getenv("PG_TEST_USE_UNIX_SOCKETS") ? true : false;
    #else
    use_unix_sockets = true;
    #endif

    // Call initialization function to set defaults
    ifunc(argc, argv);

    // Parse command-line arguments
    while ((c = getopt_long(argc, argv, "hV", long_options, &option_index)) != -1) {
        switch (c) {
            case 'h': help(); exit(0);
            case 'V': puts("pg_regress (PostgreSQL) " PG_VERSION); exit(0);
            case 1: /* dbname */ free_stringlist(&dblist); split_to_stringlist(optarg, ",", &dblist); break;
            case 2: debug = true; break;
            case 3: inputdir = pg_strdup(optarg); break;
            // ... handle other options
            default: pg_log_error_hint("Try \"%s --help\" for more information.", progname); exit(2);
        }
    }

    // Add any remaining arguments as extra tests
    while (argc - optind >= 1) {
        add_stringlist_item(&extra_tests, argv[optind]);
        optind++;
    }

    // Validate database name was specified
    if (!(dblist && dblist->str && dblist->str[0])) {
        bail("no database name was specified");
    }

    // Calculate default port for temp instances
    if (temp_instance && !port_specified_by_user) {
        port = 0xC000 | (PG_VERSION_NUM & 0x3FFF);
    }

    // Make paths absolute
    inputdir = make_absolute_path(inputdir);
    outputdir = make_absolute_path(outputdir);
    expecteddir = make_absolute_path(expecteddir);

    // Initialize test environment
    open_result_files();
    initialize_environment();

    // Set up temporary PostgreSQL instance if needed
    if (temp_instance) {
        // Create temp instance directory
        if (directory_exists(temp_instance)) {
            rmtree(temp_instance, true);
        }
        make_directory(temp_instance);

        // Initialize database (via initdb or template copy)
        if (initdb_template_dir && !nolocale && !debug) {
            // Copy from template for speed
            copy_initdb_template();
        } else {
            // Run initdb to create new instance
            run_initdb_command();
        }

        // Configure postgresql.conf for testing
        configure_postgresql_conf();

        // Start temporary postmaster
        postmaster_pid = spawn_postmaster();

        // Wait for server to accept connections
        wait_for_server_ready();

        postmaster_running = true;
    }

    // Run the actual tests
    return run_regression_tests(startfunc, postfunc);
}
```