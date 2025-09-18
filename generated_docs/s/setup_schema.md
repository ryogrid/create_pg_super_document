# setup_schema

## Location
src/bin/initdb/initdb.c: 1954 - 1973

## Overview
Loads the information schema and populates it with PostgreSQL version and SQL standard feature support information during database initialization.

## Definition


## Detailed Description
The setup_schema function is responsible for establishing the SQL standard information schema in a new PostgreSQL database. This schema provides standardized metadata views that comply with the SQL standard specification. The function performs three main operations:

1. **Schema Creation**: Executes the information schema SQL script file to create all the required views, tables, and other database objects that comprise the information schema.

2. **Version Information**: Updates the information_schema.sql_implementation_info table to record the current PostgreSQL version in the standardized format. This allows SQL clients to query the database version through standard SQL views.

3. **Feature Support Data**: Loads SQL standard feature support information from a features file into the information_schema.sql_features table. This table documents which SQL standard features are supported by this PostgreSQL installation.

The information schema provides a standardized way for applications to discover database capabilities and metadata, making PostgreSQL more compatible with SQL standard expectations.

## Parameters / Member Variables
- : FILE pointer to the command file where SQL statements are written for execution during database initialization

## Dependencies
- Functions called/Symbols referenced:
  - setup_run_file (executes SQL script files)
  - PG_CMD_PRINTF (macro for formatted SQL output)
  - escape_quotes (escapes quotes in file paths)
  - infoversion (global variable containing formatted version string)
  - info_schema_file (global variable containing path to information schema SQL file)
  - features_file (global variable containing path to SQL features data file)

- Called from:
  - initialize_data_directory (main database initialization function)

## Notes and Other Information
- The information schema is defined by the SQL standard (ISO/IEC 9075) and provides portable metadata access
- The function relies on pre-formatted version information set by set_info_version()
- Feature support data is loaded from an external file that documents PostgreSQL's SQL standard compliance
- The information schema views are read-only and provide standardized access to system catalog information
- This setup is essential for tools and applications that expect standard SQL metadata interfaces