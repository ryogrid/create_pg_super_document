# check_old_cluster_for_valid_slots

## Location
src/bin/pg_upgrade/check.c: 1928 - 2002

## Overview
Verifies that all logical replication slots in the old PostgreSQL cluster are valid and have consumed all WAL before shutdown, ensuring safe cluster upgrade.

## Definition
```c
static void check_old_cluster_for_valid_slots(bool live_check)
```

## Detailed Description
This function performs comprehensive validation of logical replication slots before PostgreSQL cluster upgrade. It iterates through all databases in the old cluster and examines each logical replication slot to ensure upgrade safety. The function checks two critical conditions: slot validity and WAL consumption status. If any issues are found, it writes problematic slots to an output file and terminates the upgrade process with detailed error messages.

The function operates in two modes based on the `live_check` parameter. When performing a live check (cluster still running), it only validates slot validity. When the cluster is shut down, it additionally verifies that all slots have consumed pending WAL, which is essential for maintaining replication consistency after upgrade.

## Parameters / Member Variables
- `live_check`: Boolean flag indicating whether the check is performed on a running cluster (true) or shut down cluster (false). When false, enables additional WAL consumption verification.

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md)
  - [LogicalSlotInfoArr](../L/LogicalSlotInfoArr.md)
  - LogicalSlotInfo
  - fopen_priv
  - [pg_log](../p/pg_log.md)
  - [check_ok](check_ok.md)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md)

## Notes and Other Information
- Creates "invalid_logical_slots.txt" file in the log base directory when problematic slots are detected
- Terminates the entire upgrade process if any invalid slots or unconsumed WAL is found
- The WAL consumption check is critical for preventing data loss during logical replication slot migration
- Part of pg_upgrade's comprehensive pre-upgrade safety validation system
- File location: src/bin/pg_upgrade/check.c:1928-2002