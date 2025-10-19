# check_for_not_null_inheritance

## Location
[src/bin/pg_upgrade/check.c:1596-1672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1596-L1672)

## Overview
Validates that child tables do not lack NOT NULL constraints that are present in their parent tables during PostgreSQL cluster upgrades.

## Definition
```c
static void check_for_not_null_inheritance(ClusterInfo *cluster)
```

## Detailed Description
This function checks for inheritance inconsistencies where child tables have columns that lack NOT NULL constraints while their corresponding parent table columns have them. Such inconsistencies were possible in PostgreSQL versions prior to version 18 but can no longer occur. The function prevents upgrade failures by identifying these problematic cases and requiring manual fixes before the upgrade can proceed.

The function performs the following operations:
- Iterates through all databases in the old cluster
- Executes a complex SQL query to identify inheritance constraint mismatches
- Writes problematic table.column combinations to a report file
- Terminates the upgrade process with detailed instructions if issues are found

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being checked

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from:
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Creates an output file "not_null_inconsistent_columns.txt" in the log base directory when issues are found
- Uses a complex SQL query joining pg_inherits, pg_attribute, pg_class, and pg_namespace system catalogs
- The function terminates the entire upgrade process if any inconsistencies are detected
- Provides specific ALTER TABLE commands to fix identified issues
- Part of PostgreSQL's cluster upgrade safety checks introduced to prevent upgrade failures

## Simplified Source

```c
static void check_for_not_null_inheritance(ClusterInfo *cluster)
{
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking for not-null constraint inconsistencies");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "not_null_inconsistent_columns.txt");

    // Check each database for inheritance NOT NULL inconsistencies
    for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
    {
        PGresult *res;
        bool db_used = false;
        DbInfo *active_db = &old_cluster.dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(&old_cluster, active_db->db_name);

        // Find child table columns that lack NOT NULL while parent has it
        res = executeQueryOrDie(conn,
                                "SELECT nspname, cc.relname, ac.attname "
                                "FROM pg_catalog.pg_inherits i, pg_catalog.pg_attribute ac, "
                                "     pg_catalog.pg_attribute ap, pg_catalog.pg_class cc, "
                                "     pg_catalog.pg_namespace nc "
                                "WHERE cc.oid = ac.attrelid AND i.inhrelid = ac.attrelid "
                                "      AND i.inhparent = ap.attrelid AND ac.attname = ap.attname "
                                "      AND cc.relnamespace = nc.oid "
                                "      AND ap.attnum > 0 and ap.attnotnull AND NOT ac.attnotnull");

        int ntup = PQntuples(res);
        int i_nspname = PQfnumber(res, "nspname");
        int i_relname = PQfnumber(res, "relname");
        int i_attname = PQfnumber(res, "attname");

        // Log any problematic columns found
        for (int i = 0; i < ntup; i++)
        {
            if (script == NULL)
                script = fopen_priv(output_path, "w");
            if (!db_used) {
                fprintf(script, "In database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  %s.%s.%s\n",
                    PQgetvalue(res, i, i_nspname),
                    PQgetvalue(res, i, i_relname),
                    PQgetvalue(res, i, i_attname));
        }

        PQclear(res);
        PQfinish(conn);
    }

    // Handle results: fail if inconsistencies found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("Your installation contains inconsistent NOT NULL constraints. "
                 "If the parent column(s) are NOT NULL, then the child column must "
                 "also be marked NOT NULL, or the upgrade will fail. "
                 "You can fix this by running "
                 "ALTER TABLE tablename ALTER column SET NOT NULL; "
                 "on each column listed in the file: %s", output_path);
    } else {
        check_ok();
    }
}
```