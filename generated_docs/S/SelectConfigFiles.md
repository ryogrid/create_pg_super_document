# SelectConfigFiles

## Location
src/backend/utils/misc/guc.c: 1786 - 1993

## Overview
SelectConfigFiles is a critical initialization function that selects and validates the configuration files and data directory to be used, and performs the initial read of postgresql.conf.

## Definition


## Detailed Description
This function is responsible for the complex process of determining and validating PostgreSQL's configuration file locations during server startup. The function performs the following key operations:

1. **Data Directory Resolution**: Determines the data directory from either the -D command-line option or the PGDATA environment variable
2. **Configuration File Location**: Locates postgresql.conf, either from command-line specification or default location in the data directory
3. **Initial Configuration Read**: Reads postgresql.conf twice - first to determine the data_directory setting, then again to include auto-configuration settings
4. **HBA and Ident File Setup**: Determines locations for pg_hba.conf and pg_ident.conf files
5. **Timezone Initialization**: Initializes timezone abbreviations if not set in configuration
6. **Path Validation**: Ensures all paths are absolute and accessible

The function implements a two-phase configuration reading strategy to handle the circular dependency between needing the data directory to find auto-configuration files and needing to read configuration to determine the data directory.

## Parameters / Member Variables
- : The -D command-line switch value specifying the data directory (NULL if not specified)
- : Program name used in error messages for user-friendly diagnostics

## Dependencies
- Functions called/Symbols referenced:
  - make_absolute_path
  - write_stderr
  - guc_malloc
  - guc_free
  - SetConfigOption
  - ProcessConfigFile
  - find_option
  - SetDataDir
  - pg_timezone_abbrev_initialize
  - CONFIG_FILENAME, HBA_FILENAME, IDENT_FILENAME (constants)
  - PGC_POSTMASTER, PGC_S_OVERRIDE (GUC constants)
- Called from (representative examples):
  - BootstrapModeMain
  - PostmasterMain
  - PostgresSingleUserMain

## Notes and Other Information
- This function is called after processing command-line switches but before full server initialization
- Returns true on success, false on failure with appropriate error messages written to stderr
- The two-phase configuration reading approach handles the bootstrap problem of needing DataDir to find PG_AUTOCONF_FILENAME
- All configuration file paths are converted to absolute paths to ensure consistent interpretation by future backends
- The function provides detailed error messages with suggestions for common configuration problems
- Memory management includes careful tracking of malloc'd vs guc_malloc'd strings
- The EXEC_BACKEND case has special considerations for transmitting DataDir to child processes