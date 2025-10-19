# check_old_cluster_subscription_state

## Location
[src/bin/pg_upgrade/check.c:2003-2117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L2003-L2117)

## Overview
Verifies that all logical replication subscriptions in the old PostgreSQL cluster have valid replication origins and that subscribed tables are in safe states for upgrade.

## Definition
```c
static void check_old_cluster_subscription_state(void)
```

## Detailed Description
This function ensures the integrity of logical replication subscriptions before PostgreSQL cluster upgrade. It performs two critical validations: first, it verifies that each subscription has a corresponding replication origin in the pg_replication_origin catalog; second, it checks that all subscribed table relations are in either 'i' (initialize) or 'r' (ready) state.

The function queries the old cluster's catalogs to identify subscriptions missing replication origins and tables in unsafe synchronization states. Unsafe states include DATASYNC, SYNCDONE, FINISHEDCOPY, and others that could leave dangling slots or origins after upgrade. When problems are detected, detailed information is written to a file and the upgrade process is terminated with comprehensive error messages.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [DbInfo](../D/DbInfo.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- Creates "subs_invalid.txt" file in the log base directory when problematic subscriptions are detected
- Only validates replication origins once (during the first database iteration) since they are cluster-wide
- Prevents upgrade when table sync states could result in dangling replication slots or origins
- Supports only 'i' (initialize) and 'r' (ready) table sync states for safe upgrade
- The origin name pattern is 'pg_' + subscription OID for validation
- Terminates upgrade process if any subscription issues are found
- File location: src/bin/pg_upgrade/check.c:2003-2117

## Simplified Source

```c
static void check_old_cluster_subscription_state(void)
{
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking for subscription state");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "subs_invalid.txt");

    // Check each database for subscription issues
    for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
    {
        PGresult *res;
        DbInfo *active_db = &old_cluster.dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(&old_cluster, active_db->db_name);

        // Check for missing replication origins (only once, cluster-wide)
        if (dbnum == 0)
        {
            // Find subscriptions missing their replication origin
            res = executeQueryOrDie(conn,
                                    "SELECT d.datname, s.subname "
                                    "FROM pg_catalog.pg_subscription s "
                                    "LEFT OUTER JOIN pg_catalog.pg_replication_origin o "
                                    "    ON o.roname = 'pg_' || s.oid "
                                    "INNER JOIN pg_catalog.pg_database d "
                                    "    ON d.oid = s.subdbid "
                                    "WHERE o.roname IS NULL;");

            int ntup = PQntuples(res);
            for (int i = 0; i < ntup; i++)
            {
                if (script == NULL)
                    script = fopen_priv(output_path, "w");
                fprintf(script, "The replication origin is missing for database:\"%s\" subscription:\"%s\"\n",
                        PQgetvalue(res, i, 0), PQgetvalue(res, i, 1));
            }
            PQclear(res);
        }

        // Check for unsafe table synchronization states
        // Only 'i' (initialize) and 'r' (ready) states are safe for upgrade
        res = executeQueryOrDie(conn,
                                "SELECT r.srsubstate, s.subname, n.nspname, c.relname "
                                "FROM pg_catalog.pg_subscription_rel r "
                                "LEFT JOIN pg_catalog.pg_subscription s "
                                "    ON r.srsubid = s.oid "
                                "LEFT JOIN pg_catalog.pg_class c "
                                "    ON r.srrelid = c.oid "
                                "LEFT JOIN pg_catalog.pg_namespace n "
                                "    ON c.relnamespace = n.oid "
                                "WHERE r.srsubstate NOT IN ('i', 'r') "
                                "ORDER BY s.subname");

        int ntup = PQntuples(res);
        for (int i = 0; i < ntup; i++)
        {
            if (script == NULL)
                script = fopen_priv(output_path, "w");
            fprintf(script, "The table sync state \"%s\" is not allowed for database:\"%s\" subscription:\"%s\" schema:\"%s\" relation:\"%s\"\n",
                    PQgetvalue(res, i, 0), active_db->db_name, PQgetvalue(res, i, 1),
                    PQgetvalue(res, i, 2), PQgetvalue(res, i, 3));
        }

        PQclear(res);
        PQfinish(conn);
    }

    // Handle results: fail if subscription issues found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("Your installation contains subscriptions without origin or having relations not in i (initialize) or r (ready) state. "
                 "You can allow the initial sync to finish for all relations and then restart the upgrade. "
                 "A list of the problematic subscriptions is in the file: %s", output_path);
    } else {
        check_ok();
    }
}
```