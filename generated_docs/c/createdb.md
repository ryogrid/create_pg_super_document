# createdb

## Location
src/backend/commands/dbcommands.c: 670 - 1556

## Overview
createdb is the main function that implements the CREATE DATABASE SQL command, responsible for creating a new PostgreSQL database by copying from a template database.

## Definition


## Detailed Description
This comprehensive function handles all aspects of database creation including option parsing, validation, permission checking, and the actual database copying process. It supports multiple creation strategies (WAL logging vs file copy) and handles complex locale, encoding, and tablespace configurations.

The function parses CREATE DATABASE statement options, validates permissions and compatibility with the template database, ensures proper encoding/locale matching, handles tablespace assignments, and coordinates the actual database creation using either WAL logging or file copying strategies.

Key operations include: option parsing and validation, permission and ownership checks, template database compatibility verification, encoding and locale validation, tablespace resolution, conflict detection, and database copying with proper error cleanup mechanisms.

## Parameters / Member Variables
- : ParseState for error reporting and parsing context
- : CreatedbStmt containing the parsed CREATE DATABASE statement with all specified options

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_info](../g/get_db_info.md), database_is_invalid_oid, CountOtherDBBackends
  - [check_encoding_locale_matches](check_encoding_locale_matches.md), check_locale, builtin_validate_locale, icu_validate_locale
  - [have_createdb_privilege](../h/have_createdb_privilege.md), check_can_set_role, object_ownercheck
  - [get_tablespace_oid](../g/get_tablespace_oid.md), GetDatabasePath, get_database_oid
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md), CreateDatabaseUsingFileCopy
  - [createdb_failure_callback](createdb_failure_callback.md), ForceSyncCommit
  - Various catalog operations: CatalogTupleInsert, recordDependencyOnOwner
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main SQL command processing)

## Notes and Other Information
- Supports two database creation strategies: CREATEDB_WAL_LOG (default) and CREATEDB_FILE_COPY
- Enforces strict compatibility requirements between new database and template (encoding, locale, collation) unless using template0
- Implements comprehensive error handling with cleanup callbacks to handle failures during the creation process
- Manages complex locale provider scenarios (libc, ICU, builtin) with proper validation and canonicalization
- Handles both explicitly assigned database OIDs and automatic OID generation with conflict detection
- template0 is treated specially as it's assumed to contain no collation-dependent data, allowing different encodings/locales
- Uses ShareLock on template database to prevent concurrent modifications during copying