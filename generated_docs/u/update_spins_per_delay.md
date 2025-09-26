# update_spins_per_delay

## Location
src/backend/storage/lmgr/s_lock.c: 224 - 246

## Overview
Updates the shared system-wide estimate of spins_per_delay using an exponential moving average when a backend process exits.

## Definition
```c
int update_spins_per_delay(int shared_spins_per_delay)
```

## Detailed Description
The `update_spins_per_delay` function implements a sophisticated algorithm to adaptively tune PostgreSQL's spinlock performance by updating the shared system estimate of optimal spin count. It uses an exponential moving average with a slow adaptation rate (15:1 ratio) to combine the current shared value with the local backend's experience.

This function is called during backend exit to contribute the local backend's spinlock tuning experience back to the shared system state. The exponential moving average ensures that noise from any single backend won't dramatically affect the system-wide setting, while still allowing gradual adaptation to changing system conditions.

The algorithm deliberately truncates rather than rounds the result, allowing small adjustments within individual backends to incrementally influence the shared estimate over time.

## Parameters / Member Variables
- `shared_spins_per_delay`: The current system-wide shared value for spins per delay that will be updated with local experience

## Dependencies
- Functions called/Symbols referenced:
  - spins_per_delay (local thread variable accessed)
- Called from (representative examples):
  - ProcKill (src/backend/storage/lmgr/proc.c:945)
  - AuxiliaryProcKill (src/backend/storage/lmgr/proc.c:1011)
- Related symbols:
  - DEFAULT_SPINS_PER_DELAY (src/include/storage/s_lock.h:815)

## Notes and Other Information
- Uses exponential moving average with 15:1 weighting (94% old value, 6% new contribution)
- Must be extremely fast as it's called while holding spinlocks
- Deliberate truncation instead of rounding allows fine-grained adjustments
- The slow adaptation rate prevents instability from noisy individual measurements
- Critical for PostgreSQL's adaptive spinlock performance optimization
- Called during both regular backend and auxiliary process termination