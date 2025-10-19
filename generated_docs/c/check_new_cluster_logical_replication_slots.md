# check_new_cluster_logical_replication_slots

## Location
[src/bin/pg_upgrade/check.c:1812-1884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1812-L1884)

## Overview
Validates that the new PostgreSQL cluster is properly configured to receive logical replication slots during an upgrade, ensuring no existing slots conflict and that required parameters are set correctly.

## Definition
```c
static void check_new_cluster_logical_replication_slots(void)
```

## Detailed Description
This function verifies that the new PostgreSQL cluster is ready to accept logical replication slots that will be migrated from the old cluster. It performs comprehensive validation to ensure the migration will succeed without conflicts or configuration issues. The function only operates when upgrading from PostgreSQL version 17 or later, as logical slot migration was introduced in that version.

The function performs the following validations:
- Checks if the old cluster has any logical replication slots to migrate
- Ensures the new cluster has no existing logical replication slots that would conflict
- Validates that wal_level is set to "logical" on the new cluster
- Verifies that max_replication_slots is sufficient to accommodate all slots from the old cluster

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION
  - [count_old_cluster_logical_slots](count_old_cluster_logical_slots.md)
  - [connectToServer](connectToServer.md)
  - [prep_status](../p/prep_status.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - [check_ok](check_ok.md)
- Called from:
  - [check_new_cluster](check_new_cluster.md)

## Notes and Other Information
- This is a static function specific to pg_upgrade functionality
- Only operates when upgrading from PostgreSQL 17 or later (logical slot migration support)
- Terminates upgrade if the new cluster has any existing logical replication slots
- Requires wal_level to be set to "logical" on the new cluster
- Validates that max_replication_slots setting can accommodate all slots from the old cluster
- Part of PostgreSQL's upgrade validation for logical replication slot migration
- Returns early if the old cluster has no logical slots to migrate
- Ensures configuration compatibility before attempting slot migration

## Simplified Source

```c
static void check_new_cluster_logical_replication_slots(void)
{
    PGresult *res;
    PGconn *conn;
    int nslots_on_old;
    int nslots_on_new;
    int max_replication_slots;
    char *wal_level;

    // Logical slots can be migrated since PG17
    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 1600)
        return;

    nslots_on_old = count_old_cluster_logical_slots();

    // Quick return if there are no logical slots to be migrated
    if (nslots_on_old == 0)
        return;

    conn = connectToServer(&new_cluster, "template1");

    prep_status("Checking for new cluster logical replication slots");

    // Ensure new cluster has no existing logical replication slots
    res = executeQueryOrDie(conn, "SELECT count(*) "
                                  "FROM pg_catalog.pg_replication_slots "
                                  "WHERE slot_type = 'logical' AND "
                                  "temporary IS FALSE;");

    if (PQntuples(res) != 1)
        pg_fatal("could not count the number of logical replication slots");

    nslots_on_new = atoi(PQgetvalue(res, 0, 0));

    if (nslots_on_new)
        pg_fatal("expected 0 logical replication slots but found %d", nslots_on_new);

    PQclear(res);

    // Check wal_level and max_replication_slots settings
    res = executeQueryOrDie(conn, "SELECT setting FROM pg_settings "
                                  "WHERE name IN ('wal_level', 'max_replication_slots') "
                                  "ORDER BY name DESC;");

    if (PQntuples(res) != 2)
        pg_fatal("could not determine parameter settings on new cluster");

    wal_level = PQgetvalue(res, 0, 0);

    // Validate wal_level is set to "logical"
    if (strcmp(wal_level, "logical") != 0)
        pg_fatal("\"wal_level\" must be \"logical\" but is set to \"%s\"", wal_level);

    max_replication_slots = atoi(PQgetvalue(res, 1, 0));

    // Ensure max_replication_slots can accommodate old cluster slots
    if (nslots_on_old > max_replication_slots)
        pg_fatal("\"max_replication_slots\" (%d) must be greater than or equal to the number of "
                 "logical replication slots (%d) on the old cluster",
                 max_replication_slots, nslots_on_old);

    PQclear(res);
    PQfinish(conn);

    check_ok();
}
```