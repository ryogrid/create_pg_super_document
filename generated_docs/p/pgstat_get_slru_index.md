# pgstat_get_slru_index

## Location
[src/backend/utils/activity/pgstat_slru.c:132-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L132-L155)

## Overview
Determines the index of an entry for a SLRU (Simple LRU) with a given name, providing a fallback to the "other" entry for external SLRUs not explicitly defined in the system.

## Definition

```c
int
pgstat_get_slru_index(const char *name)
```
## Detailed Description
This function searches through the predefined  array to find an exact match for the provided SLRU name. The function iterates through all known SLRU types including "commit_timestamp", "multixact_member", "multixact_offset", "notify", "serializable", "subtransaction", "transaction", and "other". If no exact match is found, it returns the index of the last entry ("other"), which serves as a catch-all for SLRUs defined in external projects or extensions.

## Parameters / Member Variables
- : The name of the SLRU for which to find the corresponding index

## Dependencies
- Functions called/Symbols referenced:
  -  (constant defining array size)
  -  (static array of SLRU names)
- Called from (representative examples):
  -  at src/backend/access/transam/slru.c:281
  -  at src/backend/utils/activity/pgstat_slru.c:51
  -  at src/include/pgstat.h:689

## Notes and Other Information
- The function guarantees a valid return value by falling back to the "other" entry index if no match is found
- The "other" entry must always be the last element in the  array
- This design allows external projects to track SLRU statistics even if their specific SLRU type is not predefined in PostgreSQL core
- Returns values in range [0, SLRU_NUM_ELEMENTS-1]