# check_for_tables_with_oids

## Location
[src/bin/pg_upgrade/check.c:1519-1595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1519-L1595)

## Overview
Detects tables declared WITH OIDS and blocks PostgreSQL upgrades since OID system columns are no longer supported in modern PostgreSQL versions.

## Definition
```c
static void check_for_tables_with_oids(ClusterInfo *cluster)
```

## Detailed Description
This function enforces compatibility by preventing upgrades when user tables with OID system columns exist in the source cluster. OIDs (Object Identifiers) were deprecated and removed as a table-level feature in PostgreSQL 12 due to performance overhead, maintenance complexity, and limited utility in most applications.

The function systematically scans all databases in the cluster, querying the pg_class system catalog to identify any user tables that have the relhasoids flag set to true. It specifically excludes system catalogs (pg_catalog schema) since these may legitimately use OIDs internally and are handled separately by the upgrade process.

When tables with OIDs are found, the upgrade process is halted and provides clear guidance to users on how to resolve the issue using the ALTER TABLE ... SET WITHOUT OIDS command. This allows users to remove OID columns manually before retrying the upgrade.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) - Updates status display for the validation operation
  - [connectToServer](connectToServer.md) - Establishes connections to each database in the cluster
  - [executeQueryOrDie](../e/executeQueryOrDie.md) - Executes SQL query to find tables with OID columns
  - fopen_priv - Opens output file with proper permissions for logging problematic tables
  - [PQntuples](../P/PQntuples.md), PQfnumber, PQgetvalue - PostgreSQL result set processing functions
  - [PQclear](../P/PQclear.md) - Releases PostgreSQL result set memory
  - [PQfinish](../P/PQfinish.md) - Closes database connections
  - [pg_log](../p/pg_log.md) - Logs messages at specified severity level
  - [pg_fatal](../p/pg_fatal.md) - Terminates upgrade process with fatal error message
  - [check_ok](check_ok.md) - Marks validation as successful when no issues are found
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) - Part of old cluster validation sequence

## Notes and Other Information
- This is a static function within the pg_upgrade check.c module
- Location: src/bin/pg_upgrade/check.c:1519-1595
- The SQL query uses relhasoids column from pg_class to identify tables with OID system columns
- Excludes pg_catalog schema tables as these are system-managed and handled differently during upgrades
- When issues are detected, problematic tables are logged to 'tables_with_oids.txt' in the log directory
- The error message provides specific remediation guidance using ALTER TABLE ... SET WITHOUT OIDS
- This validation ensures that deprecated table features don't cause compatibility issues in newer PostgreSQL versions
- OIDs were originally intended as unique row identifiers but are now considered obsolete in favor of proper primary keys and sequences

## Simplified Source

```c
static void check_for_tables_with_oids(ClusterInfo *cluster)
{
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking for tables WITH OIDS");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "tables_with_oids.txt");

    // Check each database for tables declared WITH OIDS
    for (int dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
    {
        PGresult *res;
        bool db_used = false;
        DbInfo *active_db = &cluster->dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(cluster, active_db->db_name);

        // Find user tables with OID system columns (excluding pg_catalog)
        res = executeQueryOrDie(conn,
                                "SELECT n.nspname, c.relname "
                                "FROM pg_catalog.pg_class c, "
                                "     pg_catalog.pg_namespace n "
                                "WHERE c.relnamespace = n.oid AND "
                                "      c.relhasoids AND "
                                "      n.nspname NOT IN ('pg_catalog')");

        int ntups = PQntuples(res);
        int i_nspname = PQfnumber(res, "nspname");
        int i_relname = PQfnumber(res, "relname");

        // Log any problematic tables found
        for (int rowno = 0; rowno < ntups; rowno++)
        {
            if (script == NULL)
                script = fopen_priv(output_path, "w");
            if (!db_used) {
                fprintf(script, "In database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  %s.%s\n",
                    PQgetvalue(res, rowno, i_nspname),
                    PQgetvalue(res, rowno, i_relname));
        }

        PQclear(res);
        PQfinish(conn);
    }

    // Handle results: fail if OID tables found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("Your installation contains tables declared WITH OIDS, which is not "
                 "supported anymore. Consider removing the oid column using "
                 "ALTER TABLE ... SET WITHOUT OIDS; "
                 "A list of tables with the problem is in the file: %s", output_path);
    } else {
        check_ok();
    }
}
```