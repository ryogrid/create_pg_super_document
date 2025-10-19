# check_for_user_defined_encoding_conversions

## Location
[src/bin/pg_upgrade/check.c:1728-1811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1728-L1811)

## Overview
Validates that the old PostgreSQL cluster does not contain any user-defined encoding conversions, which are incompatible with PostgreSQL version 14 and later due to function parameter changes.

## Definition
```c
static void check_for_user_defined_encoding_conversions(ClusterInfo *cluster)
```

## Detailed Description
This function checks for user-defined encoding conversions in the old PostgreSQL cluster that would prevent successful upgrade to version 14 or later. In PostgreSQL version 14, the conversion function parameters changed, making existing user-defined encoding conversions incompatible. The function identifies these conversions and requires their removal before upgrade can proceed.

The function performs the following operations:
- Iterates through all databases in the cluster
- Queries pg_catalog.pg_conversion for user-defined conversions (OID >= 16384)
- Joins with pg_catalog.pg_namespace to get namespace information
- Writes problematic conversions to a report file with OID, namespace, and name
- Terminates the upgrade process if any user-defined conversions are found

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing information about the PostgreSQL cluster being checked

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [connectToServer](connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - fopen_priv
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from:
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Creates an output file "encoding_conversions.txt" in the log base directory when problematic conversions are found
- Uses FirstNormalObjectId (16384) as a hardcoded cutoff to distinguish user-defined from system conversions
- The hardcoded value ensures consistent behavior regardless of future changes to the C #define
- Specifically addresses compatibility issues introduced in PostgreSQL version 14
- Provides clear guidance that conversions must be removed before upgrade can proceed
- Part of PostgreSQL's upgrade validation to prevent incompatibility with conversion function changes

## Simplified Source

```c
static void check_for_user_defined_encoding_conversions(ClusterInfo *cluster)
{
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking for user-defined encoding conversions");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "encoding_conversions.txt");

    // Check each database for user-defined encoding conversions
    for (int dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
    {
        PGresult *res;
        bool db_used = false;
        DbInfo *active_db = &cluster->dbarr.dbs[dbnum];
        PGconn *conn = connectToServer(cluster, active_db->db_name);

        // Find user-defined conversions (oid >= 16384) that would be incompatible
        res = executeQueryOrDie(conn,
                                "SELECT c.oid as conoid, c.conname, n.nspname "
                                "FROM pg_catalog.pg_conversion c, "
                                "     pg_catalog.pg_namespace n "
                                "WHERE c.connamespace = n.oid AND "
                                "      c.oid >= 16384");

        int ntups = PQntuples(res);
        int i_conoid = PQfnumber(res, "conoid");
        int i_conname = PQfnumber(res, "conname");
        int i_nspname = PQfnumber(res, "nspname");

        // Log any problematic conversions found
        for (int rowno = 0; rowno < ntups; rowno++)
        {
            if (script == NULL)
                script = fopen_priv(output_path, "w");
            if (!db_used) {
                fprintf(script, "In database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  (oid=%s) %s.%s\n",
                    PQgetvalue(res, rowno, i_conoid),
                    PQgetvalue(res, rowno, i_nspname),
                    PQgetvalue(res, rowno, i_conname));
        }

        PQclear(res);
        PQfinish(conn);
    }

    // Handle results: fail if user conversions found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("Your installation contains user-defined encoding conversions. "
                 "The conversion function parameters changed in PostgreSQL version 14 "
                 "so this cluster cannot currently be upgraded. You can remove the "
                 "encoding conversions in the old cluster and restart the upgrade. "
                 "A list of user-defined encoding conversions is in the file: %s", output_path);
    } else {
        check_ok();
    }
}
```