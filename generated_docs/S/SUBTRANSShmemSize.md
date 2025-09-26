# SUBTRANSShmemSize

## Location
src/backend/access/transam/subtrans.c: 214 - 219

## Overview
SUBTRANSShmemSize calculates the amount of shared memory required for the SUBTRANS (subtransaction) system in PostgreSQL.

## Definition

```c
Size
SUBTRANSShmemSize(void)
```
## Detailed Description
This function computes the total shared memory size needed for the SUBTRANS system, which tracks the commit status of subtransactions. The SUBTRANS system uses a Simple LRU (SLRU) buffer management scheme to cache pages of subtransaction status data. The function delegates the actual calculation to SimpleLruShmemSize, passing the number of buffers determined by SUBTRANSShmemBuffers() and 0 for the number of LSNs (since SUBTRANS doesn't track LSNs per page).

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruShmemSize
  - SUBTRANSShmemBuffers
- Called from (representative examples):
  - CalculateShmemSize

## Notes and Other Information
- Part of the SUBTRANS subsystem that manages subtransaction commit status
- The memory allocation is based on the configured or auto-tuned number of subtransaction buffers
- Used during PostgreSQL startup to determine total shared memory requirements
- Located in src/backend/access/transam/subtrans.c:214-219