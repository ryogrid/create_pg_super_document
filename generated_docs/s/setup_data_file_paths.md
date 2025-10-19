# setup_data_file_paths

## Location
[src/bin/initdb/initdb.c:2768-2811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2768-L2811)

## Overview
Establishes file paths for all essential PostgreSQL data files and templates required during database initialization, and validates their accessibility.

## Definition
void setup_data_file_paths(void)

## Detailed Description
This function configures the complete set of file paths needed for PostgreSQL database initialization. It sets up paths to critical initialization files including the bootstrap catalog interface file (postgres.bki), configuration templates (pg_hba.conf, pg_ident.conf, postgresql.conf), and various SQL files for creating system objects (information schema, system functions, views, constraints).

The function provides comprehensive diagnostic output when in debug or show-settings mode, displaying all relevant paths and configuration values. After setting up all file paths, it validates that each required file exists and is accessible, terminating the initialization process if any critical files are missing.

This function acts as a central coordinator for file path management, ensuring that all subsequent initialization steps have access to the necessary template and data files.

## Parameters / Member Variables
- Sets global file path variables:
  - `bki_file`: Bootstrap catalog interface file
  - `hba_file`: Host-based authentication configuration template
  - `ident_file`: User name map configuration template
  - `conf_file`: Main configuration file template
  - `dictionary_file`: Full-text search dictionary creation script
  - `info_schema_file`: Information schema creation script
  - `features_file`: SQL features reference file
  - `system_constraints_file`: System constraints creation script
  - `system_functions_file`: System functions creation script
  - `system_views_file`: System views creation script

## Dependencies
- Functions called/Symbols referenced:
  - [set_input](set_input.md) (PostgreSQL utility for setting file paths)
  - [check_input](../c/check_input.md) (PostgreSQL utility for validating file accessibility)
  - fprintf (C standard library)
  - exit (C standard library)
- Global variables referenced:
  - show_setting (Flag for displaying configuration)
  - [debug](../d/debug.md) (Debug mode flag)
  - PG_VERSION (PostgreSQL version constant)
  - pg_data, share_path, bin_path (Directory paths)
  - username (Database superuser name)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3465)

## Notes and Other Information
- The function sets up paths to 10 essential files required for database initialization
- All file paths are resolved relative to the established share_path directory
- When show_setting mode is enabled, the function displays comprehensive configuration information and exits
- Debug mode provides the same diagnostic output but continues execution
- File validation ensures that all required templates and data files are present before proceeding
- Missing files result in program termination with appropriate error messages
- The bootstrap catalog interface file (postgres.bki) is the most critical, containing the initial system catalog structure
- Configuration template files provide default settings that can be customized after initialization
- SQL creation scripts establish the standard PostgreSQL system objects and functionality

## Simplified Source

```c
void setup_data_file_paths(void) {
    // Set paths for all essential initialization files
    set_input(&bki_file, "postgres.bki");  // Bootstrap catalog interface
    set_input(&hba_file, "pg_hba.conf.sample");  // Authentication config
    set_input(&ident_file, "pg_ident.conf.sample");  // User mapping
    set_input(&conf_file, "postgresql.conf.sample");  // Main config
    set_input(&dictionary_file, "snowball_create.sql");  // Text search
    set_input(&info_schema_file, "information_schema.sql");  // Info schema
    set_input(&features_file, "sql_features.txt");  // SQL features reference
    set_input(&system_constraints_file, "system_constraints.sql");  // Constraints
    set_input(&system_functions_file, "system_functions.sql");  // Functions
    set_input(&system_views_file, "system_views.sql");  // Views

    // Show configuration if requested
    if (show_setting || debug) {
        fprintf(stderr,
                "VERSION=%s\nPGDATA=%s\nshare_path=%s\nPGPATH=%s\n"
                "POSTGRES_SUPERUSERNAME=%s\nPOSTGRES_BKI=%s\n"
                "POSTGRESQL_CONF_SAMPLE=%s\nPG_HBA_SAMPLE=%s\nPG_IDENT_SAMPLE=%s\n",
                PG_VERSION, pg_data, share_path, bin_path,
                username, bki_file, conf_file, hba_file, ident_file);
        if (show_setting)
            exit(0);
    }

    // Validate all files exist and are accessible
    check_input(bki_file);
    check_input(hba_file);
    check_input(ident_file);
    check_input(conf_file);
    check_input(dictionary_file);
    check_input(info_schema_file);
    check_input(features_file);
    check_input(system_constraints_file);
    check_input(system_functions_file);
    check_input(system_views_file);
}
```