# setup_data_file_paths

## Location
src/bin/initdb/initdb.c: 2768 - 2811

## Overview
Establishes file paths for all essential PostgreSQL data files and templates required during database initialization, and validates their accessibility.

## Definition
void setup_data_file_paths(void)

## Detailed Description
This function configures the complete set of file paths needed for PostgreSQL database initialization. It sets up paths to critical initialization files including the bootstrap catalog interface file (postgres.bki), configuration templates (pg_hba.conf, pg_ident.conf, postgresql.conf), and various SQL files for creating system objects (information schema, system functions, views, constraints).

The function provides comprehensive diagnostic output when in debug or show-settings mode, displaying all relevant paths and configuration values. After setting up all file paths, it validates that each required file exists and is accessible, terminating the initialization process if any critical files are missing.

This function acts as a central coordinator for file path management, ensuring that all subsequent initialization steps have access to the necessary template and data files.

## Parameters / Member Variables
- No parameters (void function)
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