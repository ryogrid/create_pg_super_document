# initialize_data_directory

## Location
[src/bin/initdb/initdb.c:3029-3142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L3029-L3142)

## Overview
The  function is the core function in PostgreSQL's initdb utility that performs the complete initialization of a new PostgreSQL data directory, creating all necessary subdirectories, configuration files, and system catalogs.

## Definition


## Detailed Description
This function orchestrates the entire PostgreSQL cluster initialization process. It performs the following major steps:

1. **Signal setup and permissions**: Sets up signal handlers and establishes proper file permissions using umask
2. **Directory structure creation**: Creates the main data directory and all required subdirectories (pg_wal, base, global, etc.)
3. **Configuration setup**: Generates initial configuration files (postgresql.conf, pg_hba.conf, etc.)
4. **Template1 bootstrap**: Initializes the template1 database using bootstrap mode
5. **System catalog initialization**: Creates system tables, views, functions, and other essential database objects
6. **Authentication and security setup**: Configures initial authentication and privilege settings
7. **Template0 and postgres databases**: Creates template0 (read-only template) and the default postgres database

The function uses a standalone backend process to execute SQL commands for post-bootstrap initialization, ensuring all system catalogs and metadata are properly established.

## Parameters / Member Variables
This function takes no parameters and operates on global variables set during command-line parsing:
- Uses  (global variable) for the target data directory path
- Uses  and  for permission settings
- Uses various global configuration settings established during option parsing

## Dependencies
- Functions called/Symbols referenced:
  - [setup_signals](../s/setup_signals.md): Sets up signal handlers
  - [create_data_directory](../c/create_data_directory.md): Creates the main data directory
  - [create_xlog_or_symlink](../c/create_xlog_or_symlink.md): Creates pg_wal directory or symlink
  - [write_version_file](../w/write_version_file.md): Creates PG_VERSION files
  - [set_null_conf](../s/set_null_conf.md): Sets initial configuration defaults
  - test_config_settings: Validates configuration settings
  - [setup_config](../s/setup_config.md): Generates configuration files
  - [bootstrap_template1](../b/bootstrap_template1.md): Initializes template1 database
  - [setup_auth](../s/setup_auth.md): Configures authentication settings
  - [setup_depend](../s/setup_depend.md): Creates system dependencies
  - [setup_privileges](../s/setup_privileges.md): Sets up initial privileges
  - [make_template0](../m/make_template0.md): Creates template0 database
  - [make_postgres](../m/make_postgres.md): Creates postgres database
- Called from:
  - [main](../m/main.md) (in initdb.c): The primary entry point for initdb utility

## Notes and Other Information
- This function is called only once during the lifetime of a PostgreSQL cluster
- It must complete successfully for the cluster to be usable
- The function creates objects in a specific order to satisfy dependencies between system catalogs
- Objects created after  are not "pinned" and can be dropped by database administrators
- Uses PG_CMD_OPEN/PG_CMD_CLOSE macros to manage communication with the standalone backend process
- Failure at any step results in a fatal error and incomplete cluster initialization