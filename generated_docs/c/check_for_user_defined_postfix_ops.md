# check_for_user_defined_postfix_ops

## Location
[src/bin/pg_upgrade/check.c:1295-1392](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1295-L1392)

## Overview
Detects and blocks PostgreSQL upgrades when user-defined postfix operators are present, as these operators are no longer supported in newer PostgreSQL versions.

## Definition
```c
static void check_for_user_defined_postfix_ops(ClusterInfo *cluster)
```

## Detailed Description
This function enforces compatibility by preventing upgrades when user-defined postfix operators exist in the source cluster. Postfix operators (operators that appear after their operand, like 'value++') were deprecated and eventually removed from PostgreSQL due to parsing ambiguities and maintenance complexity.

The function systematically scans all databases in the cluster, querying the pg_operator system catalog to find any postfix operators (identified by oprright = 0, indicating no right operand) that have OIDs >= 16384, which indicates they are user-defined rather than built-in system operators. The hardcoded value 16384 represents FirstNormalObjectId and ensures the check remains consistent even if this constant changes in future PostgreSQL versions.

When postfix operators are found, the upgrade process is halted and detailed information about each operator (including OID, namespace, name, and operand type) is logged to help users identify and migrate away from these unsupported constructs.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being validated

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) - Updates status display for the validation operation
  - [connectToServer](connectToServer.md) - Establishes connections to each database in the cluster
  - [executeQueryOrDie](../e/executeQueryOrDie.md) - Executes SQL query to find user-defined postfix operators
  - fopen_priv - Opens output file with proper permissions for logging problematic operators
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
- Location: src/bin/pg_upgrade/check.c:1295-1392
- The function uses a hardcoded FirstNormalObjectId value (16384) rather than interpolating the C #define to maintain consistency with pre-version 14 behavior
- When issues are detected, operators are logged to a file named 'postfix_ops.txt' in the log directory
- The SQL query specifically looks for operators where oprright = 0 (no right operand) and oid >= 16384 (user-defined)
- Users encountering this error must manually convert postfix operators to prefix operators or function calls before upgrading
- This validation helps ensure that deprecated language features don't cause issues in newer PostgreSQL versions

## Simplified Source

```c
static void check_for_user_defined_postfix_ops(ClusterInfo *cluster)
{
    int dbnum;
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking for user-defined postfix operators");

    // Setup output file for problematic operators
    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "postfix_ops.txt");

    // Check each database for user-defined postfix operators
    for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++) {
        PGresult *res;
        bool db_used = false;
        int ntups, rowno;
        int i_oproid, i_oprnsp, i_oprname, i_typnsp, i_typname;
        DbInfo *active_db = &cluster->dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(cluster, active_db->db_name);

        // Find user-defined postfix operators (oprright=0, oid>=16384)
        res = executeQueryOrDie(conn,
                               "SELECT o.oid AS oproid, "
                               "       n.nspname AS oprnsp, "
                               "       o.oprname, "
                               "       tn.nspname AS typnsp, "
                               "       t.typname "
                               "FROM pg_catalog.pg_operator o, "
                               "     pg_catalog.pg_namespace n, "
                               "     pg_catalog.pg_type t, "
                               "     pg_catalog.pg_namespace tn "
                               "WHERE o.oprnamespace = n.oid AND "
                               "      o.oprleft = t.oid AND "
                               "      t.typnamespace = tn.oid AND "
                               "      o.oprright = 0 AND "
                               "      o.oid >= 16384");

        ntups = PQntuples(res);
        i_oproid = PQfnumber(res, "oproid");
        i_oprnsp = PQfnumber(res, "oprnsp");
        i_oprname = PQfnumber(res, "oprname");
        i_typnsp = PQfnumber(res, "typnsp");
        i_typname = PQfnumber(res, "typname");

        // Log any problematic operators found
        for (rowno = 0; rowno < ntups; rowno++) {
            if (script == NULL) {
                script = fopen_priv(output_path, "w");
                if (!script) {
                    pg_fatal("could not open file \"%s\": %m", output_path);
                }
            }
            if (!db_used) {
                fprintf(script, "In database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  (oid=%s) %s.%s (%s.%s, NONE)\n",
                    PQgetvalue(res, rowno, i_oproid),
                    PQgetvalue(res, rowno, i_oprnsp),
                    PQgetvalue(res, rowno, i_oprname),
                    PQgetvalue(res, rowno, i_typnsp),
                    PQgetvalue(res, rowno, i_typname));
        }

        PQclear(res);
        PQfinish(conn);
    }

    if (script) {
        fclose(script);
        pg_log(PG_REPORT, "fatal");
        pg_fatal("Your installation contains user-defined postfix operators, which are not\n"
                 "supported anymore. Consider dropping the postfix operators and replacing\n"
                 "them with prefix operators or function calls.\n"
                 "A list of user-defined postfix operators is in the file:\n"
                 "    %s", output_path);
    } else {
        check_ok();
    }
}
```