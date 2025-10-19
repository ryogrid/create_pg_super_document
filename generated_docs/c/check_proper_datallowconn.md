# check_proper_datallowconn

## Location
[src/bin/pg_upgrade/check.c:1095-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1095-L1178)

## Overview
This function validates database connection settings to ensure that all non-template0 databases allow connections and that template0 explicitly disallows connections, preventing pg_dumpall restore failures.

## Definition
```c
static void check_proper_datallowconn(ClusterInfo *cluster)
```

## Detailed Description
The `check_proper_datallowconn` function performs essential validation of the `datallowconn` setting across all databases in a PostgreSQL cluster to ensure successful cluster upgrade operations. This function addresses two critical requirements:

1. **template0 Connection Restriction**: Ensures template0 database has `datallowconn=false`. If template0 allows connections, pg_dumpall will fail when attempting to restore globals because it tries to recreate template0 but cannot drop it while connections exist.

2. **Non-template0 Connection Requirement**: Ensures all other databases have `datallowconn=true`. Databases with `datallowconn=false` will be skipped during the restore process, leading to data loss.

The function queries `pg_database` to examine all databases and their connection settings. When problematic databases are found (non-template0 databases with `datallowconn=false`), it creates a report file listing these databases and terminates the upgrade with detailed instructions for resolution.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster connection and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting for user feedback)  
  - [connectToServer](connectToServer.md) (establishes database connection)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution with error handling)
  - [PQfnumber](../P/PQfnumber.md) (column index lookup)
  - [PQntuples](../P/PQntuples.md) (result tuple count)
  - [PQgetvalue](../P/PQgetvalue.md) (result value extraction)
  - [PQclear](../P/PQclear.md) (result cleanup)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - fopen_priv (secure file opening)
  - fclose (file closing)
  - [pg_log](../p/pg_log.md) (logging with severity levels)
  - [pg_fatal](../p/pg_fatal.md) (error reporting and termination)
  - [check_ok](check_ok.md) (completion status reporting)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) (old cluster validation)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- Creates a report file "databases_with_datallowconn_false.txt" in the log directory when issues are found
- Connects specifically to template1 database for querying pg_database
- The validation prevents silent data loss during cluster upgrade operations
- Template0 is treated as a special case due to pg_dumpall restore requirements
- Function provides detailed user guidance for resolving datallowconn issues
- Uses MAXPGPATH constant for output path buffer sizing

## Simplified Source

```c
static void check_proper_datallowconn(ClusterInfo *cluster)
{
    int dbnum;
    PGconn *conn_template1;
    PGresult *dbres;
    int ntups;
    int i_datname, i_datallowconn;
    FILE *script = NULL;
    char output_path[MAXPGPATH];

    prep_status("Checking database connection settings");

    // Setup output file for problematic databases
    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "databases_with_datallowconn_false.txt");

    conn_template1 = connectToServer(cluster, "template1");

    // Get all database names and their datallowconn settings
    dbres = executeQueryOrDie(conn_template1,
                             "SELECT datname, datallowconn "
                             "FROM pg_catalog.pg_database");

    i_datname = PQfnumber(dbres, "datname");
    i_datallowconn = PQfnumber(dbres, "datallowconn");

    ntups = PQntuples(dbres);
    for (dbnum = 0; dbnum < ntups; dbnum++) {
        char *datname = PQgetvalue(dbres, dbnum, i_datname);
        char *datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);

        if (strcmp(datname, "template0") == 0) {
            // template0 must NOT allow connections (for pg_dumpall restore)
            if (strcmp(datallowconn, "t") == 0) {
                pg_fatal("template0 must not allow connections, "
                         "i.e. its pg_database.datallowconn must be false");
            }
        } else {
            // All other databases MUST allow connections (or they'll be skipped)
            if (strcmp(datallowconn, "f") == 0) {
                if (script == NULL) {
                    script = fopen_priv(output_path, "w");
                    if (!script) {
                        pg_fatal("could not open file \"%s\": %m", output_path);
                    }
                }
                fprintf(script, "%s\n", datname);
            }
        }
    }

    PQclear(dbres);
    PQfinish(conn_template1);

    if (script) {
        fclose(script);
        pg_log(PG_REPORT, "fatal");
        pg_fatal("All non-template0 databases must allow connections, i.e. their\n"
                 "pg_database.datallowconn must be true. Your installation contains\n"
                 "non-template0 databases with their pg_database.datallowconn set to\n"
                 "false. Consider allowing connection for all non-template0 databases\n"
                 "or drop the databases which do not allow connections. A list of\n"
                 "databases with the problem is in the file:\n"
                 "    %s", output_path);
    } else {
        check_ok();
    }
}
```