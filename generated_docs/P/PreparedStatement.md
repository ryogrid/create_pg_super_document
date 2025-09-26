# PreparedStatement

## Location
src/include/commands/prepare.h: 34 - 61

## Overview
PreparedStatement is a data structure representing a prepared SQL statement in PostgreSQL, serving as a thin wrapper around a plancache entry with the addition of a statement name and metadata for tracking prepared statements in the database system.

## Definition


## Detailed Description
PreparedStatement represents a named, prepared SQL statement in PostgreSQL's execution system. It acts as a lightweight wrapper around the actual cached plan infrastructure, providing name-based access to prepared statements while maintaining metadata about their origin and creation time.

The structure is designed to work with PostgreSQL's hash table system (dynahash.c), which requires the key field (stmt_name) to be positioned first in the structure. The primary purpose is to associate a user-provided name with a cached plan, enabling efficient reuse of parsed and planned SQL statements.

All subsidiary storage for the actual statement parsing and planning is managed by the referenced plancache entry, making PreparedStatement primarily a naming and tracking mechanism rather than a storage container for the statement's execution components.

## Parameters / Member Variables
- : The name of the prepared statement, limited to 64 characters including null terminator. This serves as the key for hash table lookups and must be the first field for dynahash.c compatibility.
- : Pointer to the CachedPlanSource containing the actual parsed and planned statement. This is where the real execution plan and related metadata are stored.
- : Boolean flag indicating whether the statement was prepared via SQL PREPARE command (true) or through the frontend/backend protocol (false). This affects how the statement is handled during execution and cleanup.
- : Timestamp recording when the statement was initially prepared, useful for monitoring, debugging, and potential cache eviction policies.

## Dependencies
- Functions called/Symbols referenced:
  - CachedPlanSource (from utils/plancache.h)
  - TimestampTz (from datatype/timestamp.h) 
  - NAMEDATALEN (from pg_config_manual.h)
  - ParamListInfo (used in related execution functions)
- Called from (representative examples):
  - StorePreparedStatement (for creating new prepared statements)
  - FetchPreparedStatement (for retrieving existing prepared statements)
  - ExecuteQuery (during prepared statement execution)
  - DropPreparedStatement (for cleanup operations)
  - exec_bind_message (in frontend/backend protocol handling)
  - pg_prepared_statement (for system catalog access)

## Notes and Other Information
- The structure is optimized for use with PostgreSQL's dynahash hash table implementation, requiring the key field (stmt_name) to be first
- All actual statement storage and management is delegated to the plancache system through the plansource pointer
- The from_sql flag is important for distinguishing between statements prepared through different mechanisms, which may have different lifecycle management requirements
- PreparedStatement objects are typically managed through a global hash table accessible via various utility functions in commands/prepare.c
- The prepare_time field enables tracking of statement age, which could be useful for cache management or debugging performance issues
- Maximum statement name length is constrained by NAMEDATALEN (64 characters including null terminator)