# get_old_cluster_logical_slot_infos

## Location
[src/bin/pg_upgrade/info.c:640-731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L640-L731)

## Overview
The get_old_cluster_logical_slot_infos function retrieves metadata for all logical replication slots from a database in the old PostgreSQL cluster during the upgrade process.

## Definition

```c
static void
get_old_cluster_logical_slot_infos(DbInfo *dbinfo, bool live_check)
```
## Detailed Description
This function collects comprehensive information about logical replication slots from the source database during a PostgreSQL upgrade. It only operates on PostgreSQL 17 and later, as earlier versions don't reliably save logical slot state at shutdown, risking data loss. The function queries pg_replication_slots to gather slot details including name, plugin, two-phase commit status, failover capability, and validation state. For non-live checks, it determines if slots are "caught up" using the binary_upgrade_logical_slot_has_caught_up function, which checks for any pending decodable changes. Temporary and invalidated slots are handled specially - temporary slots are ignored since they cannot survive upgrades, and invalidated slots skip WAL validation as their corresponding WAL files may have been removed.

## Parameters / Member Variables
- `*dbinfo`: Pointer to DbInfo structure representing the database being processed
- `live_check`: Boolean flag indicating whether this is a live cluster check (affects caught-up determination)
## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [PQfinish](../P/PQfinish.md)
  - strcmp
- Called from (representative examples):
  - [get_db_rel_and_slot_infos](get_db_rel_and_slot_infos.md)

## Notes and Other Information
- Only functions on PostgreSQL 17+ clusters due to logical slot persistence requirements
- During live_check mode, caught_up status is always set to FALSE as new WAL records may be generated
- Intentionally skips WAL checking for invalidated slots due to potential WAL file removal
- Explicitly ignores temporary slots as they cannot survive cluster restart during upgrade
- Uses binary_upgrade_logical_slot_has_caught_up() to determine if slot is ready for migration
- Stores results in dbinfo->slot_arr structure for later validation phases
- Function is static, indicating internal use within the info.c compilation unit
- Related validation occurs in check_old_cluster_for_valid_slots() function

## Simplified Source

```c
static void
get_old_cluster_logical_slot_infos(DbInfo *dbinfo, bool live_check)
{
    PGconn *conn;
    PGresult *res;
    LogicalSlotInfo *slotinfos = NULL;
    int num_slots;

    // Only process logical slots for PG17+ (earlier versions don't persist slot state reliably)
    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 1600)
        return;

    conn = connectToServer(&old_cluster, dbinfo->db_name);

    // Query logical replication slots, checking if they're caught up for migration
    // During live_check, always mark as not caught up since new WAL may be generated
    res = executeQueryOrDie(conn,
        "SELECT slot_name, plugin, two_phase, failover, "
        "%s as caught_up, invalidation_reason IS NOT NULL as invalid "
        "FROM pg_catalog.pg_replication_slots "
        "WHERE slot_type = 'logical' AND "
        "database = current_database() AND "
        "temporary IS FALSE;",
        live_check ? "FALSE" :
        "(CASE WHEN invalidation_reason IS NOT NULL THEN FALSE "
        "ELSE (SELECT pg_catalog.binary_upgrade_logical_slot_has_caught_up(slot_name)) "
        "END)");

    num_slots = PQntuples(res);

    if (num_slots) {
        // Allocate array for slot information
        slotinfos = (LogicalSlotInfo *) pg_malloc(sizeof(LogicalSlotInfo) * num_slots);

        // Get column indices for result extraction
        int i_slotname = PQfnumber(res, "slot_name");
        int i_plugin = PQfnumber(res, "plugin");
        int i_twophase = PQfnumber(res, "two_phase");
        int i_failover = PQfnumber(res, "failover");
        int i_caught_up = PQfnumber(res, "caught_up");
        int i_invalid = PQfnumber(res, "invalid");

        // Extract slot information from query results
        for (int slotnum = 0; slotnum < num_slots; slotnum++) {
            LogicalSlotInfo *curr = &slotinfos[slotnum];

            curr->slotname = pg_strdup(PQgetvalue(res, slotnum, i_slotname));
            curr->plugin = pg_strdup(PQgetvalue(res, slotnum, i_plugin));
            curr->two_phase = (strcmp(PQgetvalue(res, slotnum, i_twophase), "t") == 0);
            curr->failover = (strcmp(PQgetvalue(res, slotnum, i_failover), "t") == 0);
            curr->caught_up = (strcmp(PQgetvalue(res, slotnum, i_caught_up), "t") == 0);
            curr->invalid = (strcmp(PQgetvalue(res, slotnum, i_invalid), "t") == 0);
        }
    }

    // Cleanup and store results
    PQclear(res);
    PQfinish(conn);

    dbinfo->slot_arr.slots = slotinfos;
    dbinfo->slot_arr.nslots = num_slots;
}
```