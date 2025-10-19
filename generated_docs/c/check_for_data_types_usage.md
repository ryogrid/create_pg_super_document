# check_for_data_types_usage

## Location
[src/bin/pg_upgrade/check.c:338-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L338-L537)

## Overview
Detects whether there are any stored columns depending on given problematic data types across all databases in a cluster and generates a report if incompatible types are found.

## Definition

```c
static void
check_for_data_types_usage(ClusterInfo *cluster, DataTypesUsageChecks *checks)
```
## Detailed Description
This function performs comprehensive data type usage validation during PostgreSQL cluster upgrades. It executes a series of configurable checks to identify columns that use data types with inconsistent on-disk representations across PostgreSQL server versions. The function uses a recursive Common Table Expression (CTE) to handle nested type dependencies including domains, arrays, composite types, and ranges that may wrap the problematic base types.

For each check that applies to the current cluster version, the function connects to every database and searches for stored columns in tables, materialized views, and indexes (but not regular views since they don't involve storage). When problematic columns are found, detailed reports are written to specified output files and the upgrade process is terminated with a fatal error.

The checks are driven by a DataTypesUsageChecks structure array that defines the metadata, SQL queries, version thresholds, and output files for each validation.

## Parameters / Member Variables
- `*cluster`: Pointer to ClusterInfo structure containing database cluster information and connection details
- `*checks`: Array of DataTypesUsageChecks structures defining the validation rules, each containing status messages, base queries, report filenames, and version hooks
## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [pg_malloc0](../p/pg_malloc0.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [pg_log](../p/pg_log.md)
  - fopen_priv
  - [PQfinish](../P/PQfinish.md)
  - [pg_free](../p/pg_free.md)
  - [check_ok](check_ok.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- Uses recursive CTE queries to handle complex type hierarchies including domains, arrays, composite types, and ranges
- Excludes temporary tables (pg_temp_*, pg_toast_temp_*) and system catalogs from checks
- Searches only stored relations (tables, materialized views, indexes) and skips views
- Terminates the upgrade process with pg_fatal() if any problematic data types are detected
- Supports version-specific checks through threshold_version and version_hook mechanisms
- Reports are appended to output files to handle findings across multiple databases

## Simplified Source

```c
static void check_for_data_types_usage(ClusterInfo *cluster, DataTypesUsageChecks *checks)
{
    bool found = false;
    bool *results;
    PQExpBufferData report;
    DataTypesUsageChecks *tmp = checks;
    int n_data_types_usage_checks = 0;

    prep_status("Checking data type usage");

    // Count number of checks to perform
    while (tmp->status != NULL) {
        n_data_types_usage_checks++;
        tmp++;
    }

    // Prepare results array
    results = pg_malloc0(sizeof(bool) * n_data_types_usage_checks);

    // Check each database in the cluster
    for (int dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++) {
        DbInfo *active_db = &cluster->dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(cluster, active_db->db_name);

        // Run all checks against current database
        for (int checknum = 0; checknum < n_data_types_usage_checks; checknum++) {
            DataTypesUsageChecks *cur_check = &checks[checknum];
            PGresult *res;
            int ntups;
            FILE *script = NULL;
            bool db_used = false;
            char output_path[MAXPGPATH];

            // Check if this validation applies to current cluster version
            if (cur_check->threshold_version == MANUAL_CHECK) {
                if (!cur_check->version_hook(cluster))
                    continue;
            } else if (cur_check->threshold_version != ALL_VERSIONS) {
                if (GET_MAJOR_VERSION(cluster->major_version) > cur_check->threshold_version)
                    continue;
            }

            snprintf(output_path, sizeof(output_path), "%s/%s",
                     log_opts.basedir, cur_check->report_filename);

            // Execute recursive CTE to find problematic data types
            // This query handles domains, arrays, composite types, and ranges
            res = executeQueryOrDie(conn,
                "WITH RECURSIVE oids AS ( "
                    "%s "  // base_query for target types
                    "UNION ALL "
                    "SELECT * FROM ( "
                        "WITH x AS (SELECT oid FROM oids) "
                        // Find domains, arrays, composite types, ranges containing target types
                        "SELECT t.oid FROM pg_catalog.pg_type t, x WHERE typbasetype = x.oid AND typtype = 'd' "
                        "UNION ALL "
                        "SELECT t.oid FROM pg_catalog.pg_type t, x WHERE typelem = x.oid AND typtype = 'b' "
                        "UNION ALL "
                        "SELECT t.oid FROM pg_catalog.pg_type t, pg_catalog.pg_class c, pg_catalog.pg_attribute a, x "
                        "WHERE t.typtype = 'c' AND t.oid = c.reltype AND c.oid = a.attrelid AND "
                              "NOT a.attisdropped AND a.atttypid = x.oid "
                        "UNION ALL "
                        "SELECT t.oid FROM pg_catalog.pg_type t, pg_catalog.pg_range r, x "
                        "WHERE t.typtype = 'r' AND r.rngtypid = t.oid AND r.rngsubtype = x.oid"
                    ") foo "
                ") "
                // Find stored columns using any of these types
                "SELECT n.nspname, c.relname, a.attname "
                "FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n, pg_catalog.pg_attribute a "
                "WHERE c.oid = a.attrelid AND NOT a.attisdropped AND "
                      "a.atttypid IN (SELECT oid FROM oids) AND "
                      "c.relkind IN ('r', 'm', 'i') AND "  // tables, matviews, indexes
                      "c.relnamespace = n.oid AND "
                      "n.nspname !~ '^pg_temp_' AND n.nspname !~ '^pg_toast_temp_' AND "
                      "n.nspname NOT IN ('pg_catalog', 'information_schema')",
                cur_check->base_query);

            ntups = PQntuples(res);

            // If problematic columns found, write report
            if (ntups) {
                if (!found) {
                    initPQExpBuffer(&report);
                    found = true;
                }

                if (!results[checknum]) {
                    pg_log(PG_REPORT, "failed check: %s", _(cur_check->status));
                    appendPQExpBuffer(&report, "\n%s\n%s\n    %s\n",
                                      _(cur_check->report_text),
                                      _("A list of the problem columns is in the file:"),
                                      output_path);
                    results[checknum] = true;
                }

                // Write problematic columns to output file
                int i_nspname = PQfnumber(res, "nspname");
                int i_relname = PQfnumber(res, "relname");
                int i_attname = PQfnumber(res, "attname");

                for (int rowno = 0; rowno < ntups; rowno++) {
                    if (script == NULL && (script = fopen_priv(output_path, "a")) == NULL)
                        pg_fatal("could not open file \"%s\": %m", output_path);

                    if (!db_used) {
                        fprintf(script, "In database: %s\n", active_db->db_name);
                        db_used = true;
                    }
                    fprintf(script, "  %s.%s.%s\n",
                            PQgetvalue(res, rowno, i_nspname),
                            PQgetvalue(res, rowno, i_relname),
                            PQgetvalue(res, rowno, i_attname));
                }

                if (script) {
                    fclose(script);
                    script = NULL;
                }
            }

            PQclear(res);
        }

        PQfinish(conn);
    }

    // Terminate upgrade if problems found
    if (found)
        pg_fatal("Data type checks failed: %s", report.data);

    pg_free(results);
    check_ok();
}
```