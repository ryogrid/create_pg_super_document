# SyncStandbySlotsConfigData

## Location
[src/backend/replication/slot.c:100-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L100-L112)

## Overview
Configuration data structure for synchronized standby slots, designed to store a list of slot names that can be synchronized from the primary to standby servers.

## Definition
```c
typedef struct
{
    /* Number of slot names in the slot_names[] */
    int         nslotnames;

    /*
     * slot_names contains 'nslotnames' consecutive null-terminated C strings.
     */
    char        slot_names[FLEXIBLE_ARRAY_MEMBER];
} SyncStandbySlotsConfigData;
```

## Detailed Description
SyncStandbySlotsConfigData is a configuration structure used to manage the `synchronized_standby_slots` GUC (Grand Unified Configuration) parameter in PostgreSQL. This structure stores the names of replication slots that should be synchronized between primary and standby servers in a streaming replication setup.

The structure is designed as a flat representation that can be held in a single chunk of `guc_malloc`\d memory, allowing it to be stored as the extra data for the synchronized_standby_slots GUC. This design ensures efficient memory management and compatibility with PostgreSQL\s configuration system.

The slot names are stored as consecutive null-terminated C strings in the flexible array member `slot_names`, with `nslotnames` indicating how many slot names are present.

## Parameters / Member Variables
- `nslotnames`: Integer count of slot names stored in the slot_names array
- `slot_names`: Flexible array member containing consecutive null-terminated C strings representing the names of slots to be synchronized

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array members)
- Called from (representative examples):
  - [check_synchronized_standby_slots](../c/check_synchronized_standby_slots.md) (src/backend/replication/slot.c:2495)
  - [assign_synchronized_standby_slots](../a/assign_synchronized_standby_slots.md) (src/backend/replication/slot.c:2552)
  - [SlotExistsInSyncStandbySlots](SlotExistsInSyncStandbySlots.md) (src/backend/replication/slot.c:2564)

## Notes and Other Information
- This structure is used specifically for the synchronized_standby_slots GUC parameter
- The structure must remain flat to be compatible with GUC memory management (guc_malloc)
- Used in conjunction with failover slots and replication slot synchronization features
- The synchronized_standby_slots_config global variable holds the current configuration
- Memory allocation and parsing logic is handled in check_synchronized_standby_slots function
- Part of PostgreSQL's high availability and disaster recovery functionality