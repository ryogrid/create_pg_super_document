# setup_privileges

## Location
src/bin/initdb/initdb.c: 1786 - 1926

## Overview
Sets up default privileges for system catalogs and populates the pg_init_privs table to preserve privilege information at database initialization time.

## Definition


## Detailed Description
The setup_privileges function is responsible for establishing initial access permissions on PostgreSQL system catalogs during database initialization. It performs two main tasks:

1. **System Catalog Privileges**: Marks most system catalogs as world-readable by updating their Access Control Lists (ACLs). The function carefully preserves any existing privilege sets that have already been configured (NOT NULL values).

2. **pg_init_privs Population**: Populates the pg_init_privs system catalog with the initial privilege state of database objects. This information is crucial for pg_dump to preserve user-modified privileges across dump/reload operations and pg_upgrade processes.

The function handles privileges for various object types including relations (tables, views, materialized views, sequences), attributes (columns), procedures, types, languages, large objects, namespaces, foreign data wrappers, and foreign servers. Note that databases and tablespaces are excluded since pg_init_privs only tracks per-database objects.

## Parameters / Member Variables
- : FILE pointer to the command file where SQL statements are written for execution during database initialization

## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PRINTF (macro for formatted SQL output)
  - PG_CMD_PUTS (macro for SQL string output)
  - escape_quotes (function to escape quotes in strings)
  - CppAsString2 (macro for stringifying constants)
  - RELKIND_* constants (relation kind identifiers)
  - BOOTSTRAP_SUPERUSERID constant

- Called from:
  - initialize_data_directory (main initialization function)

## Notes and Other Information
- This function is critical for PostgreSQL security model initialization
- The privilege setup ensures backward compatibility for pg_dump and pg_upgrade operations
- The function uses SQL commands written to cmdfd rather than direct database API calls
- Special handling is provided for large objects, which have their public access revoked by default
- The 'i' privtype in pg_init_privs indicates initial/installation privileges