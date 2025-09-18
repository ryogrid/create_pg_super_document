# print_slot_infos

## Location
[src/bin/pg_upgrade/info.c:826-843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/info.c#L826-L843)

## Overview
Prints detailed information about logical replication slots in a LogicalSlotInfoArr structure for debugging during pg_upgrade.

## Definition
```c
static void print_slot_infos(LogicalSlotInfoArr *slot_arr)
```

## Detailed Description
This function provides verbose logging output for logical replication slot information during the pg_upgrade process. It first checks if there are any slots to process and returns early if none exist. If slots are present, it logs a header message and then iterates through all slots in the array, printing detailed information about each slot including the slot name, output plugin, and two-phase commit capability. This debugging information is crucial for administrators to understand which replication slots are being processed and their configuration during the upgrade.

## Parameters / Member Variables
- `slot_arr`: Pointer to LogicalSlotInfoArr structure containing logical replication slot information to be printed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_log](pg_log.md)
  - [LogicalSlotInfoArr](../L/LogicalSlotInfoArr.md) (struct type)
  - LogicalSlotInfo (struct type)
  - PG_VERBOSE (log level constant)
- Called from (representative examples):
  - [print_db_infos](print_db_infos.md)

## Notes and Other Information
- This is a static function only used within src/bin/pg_upgrade/info.c
- Uses PG_VERBOSE logging level, so output is only visible when verbose logging is enabled
- Quick return optimization when no slots exist (slot_arr->nslots == 0)
- Output format includes slot name, output plugin, and two_phase status ("true"/"false")
- Slot names and plugin names are quoted in the output for clarity
- Part of the pg_upgrade utility's debugging system for logical replication slots
- Essential for troubleshooting upgrade issues related to logical replication