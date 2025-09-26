# check_for_incompatible_polymorphics

## Location
[src/bin/pg_upgrade/check.c:1393-1518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1393-L1518)

## Overview
Detects user-defined objects that reference deprecated polymorphic functions using anyarray/anyelement arguments, blocking upgrades until they're migrated to anycompatible variants.

## Definition
```c
static void check_for_incompatible_polymorphics(ClusterInfo *cluster)
```

## Detailed Description
This function addresses a significant compatibility issue introduced when PostgreSQL enhanced its polymorphic type system by adding new "anycompatible" family types (anycompatible, anycompatiblearray, etc.). The older anyarray/anyelement polymorphic types were retained for backward compatibility but internal functions were migrated to use the new types for better type safety and consistency.

The function builds a version-dependent list of problematic internal functions that were changed from anyarray/anyelement to anycompatiblearray/anycompatible signatures. It then systematically scans all databases to find user-defined objects (aggregates, operators) that still reference these old function signatures. The search covers:

1. Aggregate transition functions using old polymorphic types
2. Aggregate final functions using old polymorphic types  
3. Operators using old polymorphic function implementations

The function dynamically constructs the list of problematic functions based on the cluster's PostgreSQL version, as different functions were introduced in different versions (9.3 added array_remove/array_replace, 9.5 added array_position functions, etc.).

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) - Updates status display for the validation operation
  - [initPQExpBuffer](../i/initPQExpBuffer.md) - Initializes dynamic string buffer for building function list
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md) - Appends strings to the dynamic buffer
  - GET_MAJOR_VERSION - Extracts major version number from cluster version
  - [connectToServer](connectToServer.md) - Establishes connections to each database in the cluster
  - [executeQueryOrDie](../e/executeQueryOrDie.md) - Executes complex SQL query to find problematic objects
  - fopen_priv - Opens output file with proper permissions for logging issues
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue - PostgreSQL result set processing functions
  - [PQclear](../P/PQclear.md) - Releases PostgreSQL result set memory
  - [PQfinish](../P/PQfinish.md) - Closes database connections
  - [pg_log](../p/pg_log.md) - Logs messages at specified severity level
  - [pg_fatal](../p/pg_fatal.md) - Terminates upgrade process with fatal error message
  - [check_ok](check_ok.md) - Marks validation as successful when no issues are found
  - [termPQExpBuffer](../t/termPQExpBuffer.md) - Cleans up dynamic string buffer
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) - Part of old cluster validation sequence

## Notes and Other Information
- This is a static function within the pg_upgrade check.c module
- Location: src/bin/pg_upgrade/check.c:1393-1518
- The function uses version-specific logic to build appropriate lists of problematic functions based on when they were introduced
- Uses hardcoded FirstNormalObjectId value (16384) to distinguish user-defined objects from system objects
- The complex SQL query searches across multiple system catalogs (pg_proc, pg_aggregate, pg_operator) to find all references
- When issues are detected, problematic objects are logged to 'incompatible_polymorphics.txt' in the log directory
- Users must manually drop and recreate affected objects with updated function references before upgrading
- This check ensures that polymorphic type system changes don't cause runtime failures after upgrade