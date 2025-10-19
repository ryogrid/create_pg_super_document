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

## Simplified Source

```c
static void check_for_incompatible_polymorphics(ClusterInfo *cluster)
{
    PGresult *res;
    FILE *script = NULL;
    char output_path[MAXPGPATH];
    PQExpBufferData old_polymorphics;

    prep_status("Checking for incompatible polymorphic functions");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "incompatible_polymorphics.txt");

    // Build list of problematic functions based on PostgreSQL version
    initPQExpBuffer(&old_polymorphics);

    // Core functions available in all versions
    appendPQExpBufferStr(&old_polymorphics,
                         "'array_append(anyarray,anyelement)'"
                         ", 'array_cat(anyarray,anyarray)'"
                         ", 'array_prepend(anyelement,anyarray)'");

    // Add version-specific functions
    if (GET_MAJOR_VERSION(cluster->major_version) >= 903)
        appendPQExpBufferStr(&old_polymorphics,
                             ", 'array_remove(anyarray,anyelement)'"
                             ", 'array_replace(anyarray,anyelement,anyelement)'");

    if (GET_MAJOR_VERSION(cluster->major_version) >= 905)
        appendPQExpBufferStr(&old_polymorphics,
                             ", 'array_position(anyarray,anyelement)'"
                             ", 'array_position(anyarray,anyelement,integer)'"
                             ", 'array_positions(anyarray,anyelement)'"
                             ", 'width_bucket(anyelement,anyarray)'");

    // Check each database for problematic user-defined objects
    for (int dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
    {
        bool db_used = false;
        DbInfo *active_db = &cluster->dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(cluster, active_db->db_name);

        // Query finds user objects (oid >= 16384) that reference old polymorphic functions
        res = executeQueryOrDie(conn,
            // Check aggregate transition functions
            "SELECT 'aggregate' AS objkind, p.oid::regprocedure::text AS objname "
            "FROM pg_proc AS p "
            "JOIN pg_aggregate AS a ON a.aggfnoid=p.oid "
            "WHERE p.oid >= 16384 "
            "AND a.aggtransfn = ANY(ARRAY[%s]::regprocedure[]) "
            "AND a.aggtranstype = ANY(ARRAY['anyarray', 'anyelement']::regtype[]) "

            // Check aggregate final functions
            "UNION ALL "
            "SELECT 'aggregate' AS objkind, p.oid::regprocedure::text AS objname "
            "FROM pg_proc AS p "
            "JOIN pg_aggregate AS a ON a.aggfnoid=p.oid "
            "WHERE p.oid >= 16384 "
            "AND a.aggfinalfn = ANY(ARRAY[%s]::regprocedure[]) "
            "AND a.aggtranstype = ANY(ARRAY['anyarray', 'anyelement']::regtype[]) "

            // Check operators
            "UNION ALL "
            "SELECT 'operator' AS objkind, op.oid::regoperator::text AS objname "
            "FROM pg_operator AS op "
            "WHERE op.oid >= 16384 "
            "AND oprcode = ANY(ARRAY[%s]::regprocedure[]);",
            old_polymorphics.data, old_polymorphics.data, old_polymorphics.data);

        int ntups = PQntuples(res);
        int i_objkind = PQfnumber(res, "objkind");
        int i_objname = PQfnumber(res, "objname");

        // Log any problematic objects found
        for (int rowno = 0; rowno < ntups; rowno++)
        {
            if (script == NULL)
                script = fopen_priv(output_path, "w");
            if (!db_used) {
                fprintf(script, "In database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  %s: %s\n",
                    PQgetvalue(res, rowno, i_objkind),
                    PQgetvalue(res, rowno, i_objname));
        }

        PQclear(res);
        PQfinish(conn);
    }

    // Handle results: fail if problems found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("User-defined objects refer to internal polymorphic functions "
                 "with anyarray/anyelement arguments. These must be dropped "
                 "before upgrading. See: %s", output_path);
    } else {
        check_ok();
    }

    termPQExpBuffer(&old_polymorphics);
}
```