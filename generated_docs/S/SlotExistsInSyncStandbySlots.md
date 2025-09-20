# SlotExistsInSyncStandbySlots

## Location
[src/backend/replication/slot.c:2559-2591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L2559-L2591)

## Overview
Checks if a given slot name is specified in the synchronized_standby_slots GUC parameter configuration.

## Definition

```c
bool
SlotExistsInSyncStandbySlots(const char *slot_name)
```
## Detailed Description
This function performs a linear search through the configured synchronized standby slots to determine if a specific slot name is included in the synchronized_standby_slots GUC parameter. It accesses the global synchronized_standby_slots_config structure that contains the parsed and validated slot names. The function uses a simple linear search algorithm, iterating through the packed slot names in the configuration structure. Each slot name is null-terminated and stored contiguously in memory, so the function advances the pointer by the string length plus one to move to the next slot name.

## Parameters / Member Variables
- `slot_name`: The name of the replication slot to search for in the synchronized standby slots configuration

## Dependencies
- Functions called/Symbols referenced:
  - synchronized_standby_slots_config (global configuration structure)
  - strcmp (for string comparison)
  - strlen (for string length calculation)
- Called from (representative examples):
  - [PhysicalWakeupLogicalWalSnd](../P/PhysicalWakeupLogicalWalSnd.md) (in walsender.c)

## Notes and Other Information
- Returns false immediately if synchronized_standby_slots_config is NULL (no slots configured)
- Uses linear search algorithm with O(n) complexity where n is the number of configured slots
- The code includes a comment noting that linear search is acceptable for expected small list sizes, but caching could be added if needed
- Slot names are stored in a packed format where each name is null-terminated and stored contiguously
- This function is used by the WAL sender process to determine if physical replication slots should wake up logical WAL senders