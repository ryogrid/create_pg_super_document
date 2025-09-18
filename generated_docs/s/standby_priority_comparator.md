# standby_priority_comparator

## Location
[src/backend/replication/syncrep.c:833-859](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L833-L859)

## Overview
A qsort comparator function that sorts SyncRepStandbyData entries by their synchronous standby priority values in ascending order.

## Definition
```c
static int standby_priority_comparator(const void *a, const void *b)
```

## Detailed Description
This function serves as a comparison function for qsort to order synchronous standby candidates by their priority values. It implements a two-level sorting strategy:

1. Primary sort: Orders standbys by increasing sync_standby_priority value (lower numbers = higher priority)
2. Secondary sort: For standbys with equal priority values, breaks ties using their walsnd_index (position in the WalSnd array)

The tie-breaking mechanism using walsnd_index is noted as "utterly bogus" in the comments since it depends on arrival order, but it's maintained for regression test compatibility.

## Parameters / Member Variables
- `a`: Pointer to first SyncRepStandbyData structure to compare
- `b`: Pointer to second SyncRepStandbyData structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - SyncRepStandbyData (structure being compared)
- Called from (representative examples):
  - SyncStandbysDefined (src/backend/replication/syncrep.c:120)
  - [SyncRepGetCandidateStandbys](../S/SyncRepGetCandidateStandbys.md) (src/backend/replication/syncrep.c:821)

## Notes and Other Information
- Returns negative value if a < b, positive if a > b, zero if equal
- Used specifically in priority-based synchronous replication mode
- The tie-breaking behavior is explicitly acknowledged as suboptimal but preserved for test compatibility
- Static function scope limits visibility to the syncrep.c compilation unit