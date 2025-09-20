# create_database

## Location
[src/test/regress/pg_regress.c:1955-1988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1955-L1988)

## Overview
Creates a new PostgreSQL database with standardized settings optimized for regression testing, including locale configuration and extension installation.

## Definition

```c
static void
create_database(const char *dbname)
```
## Detailed Description
This function creates a new database specifically configured for PostgreSQL regression testing. It uses template0 as the base template to avoid any installation-local customizations that might affect test results. The function applies consistent locale and configuration settings to ensure reproducible test behavior across different environments.

The function performs the following operations:
1. Creates the database using template0 as the base template
2. Optionally specifies encoding if provided
3. Sets locale to 'C' if nolocale option is enabled
4. Standardizes locale-specific settings (lc_messages, lc_monetary, lc_numeric, lc_time)
5. Sets bytea_output to 'hex' for consistent binary data representation
6. Sets timezone_abbreviations to 'Default'
7. Installs any requested extensions using CREATE EXTENSION IF NOT EXISTS

## Parameters / Member Variables
- : Name of the database to create

## Dependencies
- Functions called/Symbols referenced:
  - [psql_start_command](../p/psql_start_command.md) (initialize psql command buffer)
  - [psql_add_command](../p/psql_add_command.md) (add SQL commands to buffer)
  - [psql_end_command](../p/psql_end_command.md) (execute the buffered commands)
  - psql_command (execute single SQL command)
- Global variables referenced:
  - encoding (optional character encoding setting)
  - nolocale (flag to force C locale)
  - loadextension (list of extensions to install)
- Called from (representative examples):
  - No direct callers found in codebase

## Notes and Other Information
- This is a static function used internally by the pg_regress test framework
- Uses template0 instead of template1 to ensure clean, predictable database state
- Standardizes locale settings to 'C' for consistent test results across different systems
- Sets bytea_output to 'hex' to ensure consistent binary data representation in test output
- Supports conditional encoding specification for testing different character sets
- Automatically installs requested extensions with IF NOT EXISTS for idempotency
- Part of PostgreSQL's test database setup infrastructure