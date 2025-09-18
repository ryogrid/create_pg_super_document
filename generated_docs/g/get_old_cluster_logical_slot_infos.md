# get_old_cluster_logical_slot_infos

## Location
src/bin/pg_upgrade/info.c: 640 - 731

## Overview
The get_old_cluster_logical_slot_infos function retrieves metadata for all logical replication slots from a database in the old PostgreSQL cluster during the upgrade process.

## Definition


## Detailed Description
This function collects comprehensive information about logical replication slots from the source database during a PostgreSQL upgrade. It only operates on PostgreSQL 17 and later, as earlier versions don't reliably save logical slot state at shutdown, risking data loss. The function queries pg_replication_slots to gather slot details including name, plugin, two-phase commit status, failover capability, and validation state. For non-live checks, it determines if slots are "caught up" using the binary_upgrade_logical_slot_has_caught_up function, which checks for any pending decodable changes. Temporary and invalidated slots are handled specially - temporary slots are ignored since they cannot survive upgrades, and invalidated slots skip WAL validation as their corresponding WAL files may have been removed.

## Parameters / Member Variables
- : Pointer to DbInfo structure representing the database being processed
- : Boolean flag indicating whether this is a live cluster check (affects caught-up determination)

## Dependencies
- Functions called/Symbols referenced:
  - GET_MAJOR_VERSION
  - [connectToServer](../c/connectToServer.md)
  - [executeQueryOrDie](../e/executeQueryOrDie.md)
  - pg_malloc
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