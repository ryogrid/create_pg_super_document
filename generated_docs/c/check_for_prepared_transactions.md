# check_for_prepared_transactions

## Location
[src/bin/pg_upgrade/check.c:1179-1213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1179-L1213)

## Overview
Validates that no prepared transactions exist in a PostgreSQL cluster during the pg_upgrade process, ensuring compatibility for database upgrades.

## Definition
```c
static void check_for_prepared_transactions(ClusterInfo *cluster)
```

## Detailed Description
This function is a critical validation step in PostgreSQL's pg_upgrade utility. It ensures that both source and target clusters are free of prepared transactions before proceeding with an upgrade. Prepared transactions (two-phase commit transactions) may have storage format dependencies that could be incompatible between PostgreSQL versions, making their presence a blocking condition for upgrades.

The function connects to the template1 database and queries the pg_catalog.pg_prepared_xacts system view to detect any existing prepared transactions. If any are found, the upgrade process is terminated with a fatal error, requiring manual intervention to either commit or rollback the prepared transactions before retrying the upgrade.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing connection information for the PostgreSQL cluster being checked (either source or target)

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](connectToServer.md) - Establishes connection to the specified cluster
  - [prep_status](../p/prep_status.md) - Updates status display for the current check operation  
  - [executeQueryOrDie](../e/executeQueryOrDie.md) - Executes the prepared transaction query with error handling
  - [PQclear](../P/PQclear.md) - Releases PostgreSQL result set memory
  - [PQfinish](../P/PQfinish.md) - Closes the database connection
  - [check_ok](check_ok.md) - Marks the validation step as successful
  - [pg_fatal](../p/pg_fatal.md) - Terminates upgrade with fatal error message
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) - Part of old cluster validation sequence
  - [check_new_cluster](check_new_cluster.md) - Part of new cluster validation sequence

## Notes and Other Information
- This is a static function within the pg_upgrade check.c module, indicating it's only used internally within that compilation unit
- The function operates on both old and new clusters during the upgrade process, providing different error messages for each case
- Location: src/bin/pg_upgrade/check.c:1179-1213
- The check uses the standard PostgreSQL system catalog pg_prepared_xacts which contains information about all currently prepared transactions
- This validation is mandatory - prepared transactions must be manually resolved before pg_upgrade can proceed

## Simplified Source

```c
static void check_for_prepared_transactions(ClusterInfo *cluster)
{
    PGresult *res;
    PGconn *conn = connectToServer(cluster, "template1");

    prep_status("Checking for prepared transactions");

    // Query for any existing prepared transactions
    res = executeQueryOrDie(conn,
                           "SELECT * "
                           "FROM pg_catalog.pg_prepared_xacts");

    // Fail if any prepared transactions exist
    if (PQntuples(res) != 0) {
        if (cluster == &old_cluster) {
            pg_fatal("The source cluster contains prepared transactions");
        } else {
            pg_fatal("The target cluster contains prepared transactions");
        }
    }

    PQclear(res);
    PQfinish(conn);
    check_ok();
}
```