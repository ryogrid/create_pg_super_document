bootstrap_template1

## Overview
Executes the BKI (Backend Interface) bootstrap script to create the template1 database, which serves as the foundation for all other databases in a PostgreSQL cluster.

## Definition
static void bootstrap_template1(void)

## Detailed Description
This critical function runs the PostgreSQL bootstrap process that creates the template1 database by executing a BKI script in bootstrap mode. The BKI script contains SQL-like commands that define the initial system catalogs, tables, functions, and other database objects that form the core of PostgreSQL.

The function performs several important operations:
1. Reads and validates the BKI file to ensure it matches the current PostgreSQL version
2. Performs token substitution to customize the BKI script with installation-specific values (username, encoding, locale settings, etc.)
3. Constructs and executes a backend command in bootstrap mode with appropriate options
4. Feeds the processed BKI script lines to the backend process
5. Creates the template1 database which will be used as the basis for creating other databases

The token substitution mechanism replaces placeholders in the BKI file with actual values such as NAMEDATALEN, SIZEOF_POINTER, encoding information, username, and locale settings. This allows the same BKI template to work across different PostgreSQL configurations and platforms.

## Parameters / Member Variables
This function takes no parameters but operates on numerous global variables including:
- bki_file: Path to the BKI script file
- backend_exec: Path to the PostgreSQL backend executable
- username, encodingid: Database user and encoding settings
- lc_collate, lc_ctype, datlocale, icu_rules: Locale configuration
- data_checksums, debug: Various initialization options

## Dependencies
- Functions called/Symbols referenced:
  - [readfile](../r/readfile.md), replace_token
  - [escape_quotes_bki](../e/escape_quotes_bki.md), encodingid_to_string
  - [initPQExpBuffer](../i/initPQExpBuffer.md), printfPQExpBuffer, termPQExpBuffer
  - PG_CMD_OPEN, PG_CMD_PUTS, PG_CMD_CLOSE
  - [check_ok](../c/check_ok.md), pg_log_error, pg_log_error_hint
- Called from (representative examples):
  - [initialize_data_directory](../i/initialize_data_directory.md) (around line 3082)

## Notes and Other Information
- This is a static function within initdb.c, used specifically during database cluster initialization
- The function validates the BKI file version by checking the header line against the current PostgreSQL major version
- Token replacement is used extensively to customize the BKI script for the specific installation
- The backend is run in bootstrap mode (--boot flag) which is a special mode for initial database creation
- WAL segment size is specified via the -X parameter during bootstrap
- Data checksums can be enabled via the -k parameter if requested
- Debug output can be enabled with -d 5 for troubleshooting
- The PGCLIENTENCODING environment variable is cleared to avoid confusion during bootstrap
- Creates the foundation template1 database which will be copied to create other databases
- This process is essential and must complete successfully for the cluster initialization to succeed