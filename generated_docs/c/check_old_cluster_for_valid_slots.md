# check_old_cluster_for_valid_slots

## Location
[src/bin/pg_upgrade/check.c:1928-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1928-L2002)

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

## Simplified Source

```c
static void check_old_cluster_for_valid_slots(bool live_check)
{
    char output_path[MAXPGPATH];
    FILE *script = NULL;

    prep_status("Checking for valid logical replication slots");

    snprintf(output_path, sizeof(output_path), "%s/%s",
             log_opts.basedir, "invalid_logical_slots.txt");

    // Check all logical replication slots in each database
    for (int dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
    {
        LogicalSlotInfoArr *slot_arr = &old_cluster.dbarr.dbs[dbnum].slot_arr;

        for (int slotnum = 0; slotnum < slot_arr->nslots; slotnum++)
        {
            LogicalSlotInfo *slot = &slot_arr->slots[slotnum];

            // Check if the slot is valid/usable
            if (slot->invalid)
            {
                if (script == NULL)
                    script = fopen_priv(output_path, "w");
                fprintf(script, "The slot \"%s\" is invalid\n", slot->slotname);
                continue;
            }

            // Additional check for WAL consumption (only when cluster is shut down)
            // This ensures all pending WAL has been consumed before upgrade
            if (!live_check && !slot->caught_up)
            {
                if (script == NULL)
                    script = fopen_priv(output_path, "w");
                fprintf(script, "The slot \"%s\" has not consumed the WAL yet\n",
                        slot->slotname);
            }
        }
    }

    // Handle results: fail if problematic slots found, otherwise mark success
    if (script) {
        fclose(script);
        pg_fatal("Your installation contains logical replication slots that cannot be upgraded. "
                 "You can remove invalid slots and/or consume the pending WAL for other slots, "
                 "and then restart the upgrade. "
                 "A list of the problematic slots is in the file: %s", output_path);
    }

    check_ok();
}
```