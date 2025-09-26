# SUBTRANSShmemInit

## Location
[src/backend/access/transam/subtrans.c:220-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L220-L253)

## Overview
SUBTRANSShmemInit initializes the shared memory structures for the SUBTRANS (subtransaction) system in PostgreSQL.

## Definition
```c
void SUBTRANSShmemInit(void)
```

## Detailed Description
This function performs the initialization of the SUBTRANS shared memory subsystem during PostgreSQL startup. It first handles auto-tuning of the subtransaction_buffers configuration parameter if it was set to 0, calculating an appropriate buffer count and updating the configuration. Then it initializes the SimpleLru control structure (SubTransCtl) with the appropriate parameters for managing subtransaction status pages. The function also sets up the page precedence function and performs unit tests on the SLRU page precedence logic.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SUBTRANSShmemBuffers
  - SetConfigOption
  - SimpleLruInit
  - SlruPagePrecedesUnitTests
  - SubTransPagePrecedes
- Global variables accessed:
  - SubTransCtl
  - subtransaction_buffers
- Constants used:
  - PGC_POSTMASTER
  - PGC_S_DYNAMIC_DEFAULT
  - PGC_S_OVERRIDE
  - LWTRANCHE_SUBTRANS_BUFFER
  - LWTRANCHE_SUBTRANS_SLRU
  - SYNC_HANDLER_NONE
  - SUBTRANS_XACTS_PER_PAGE
- Called from (representative examples):
  - CreateOrAttachShmemStructs

## Notes and Other Information
- Handles auto-tuning of subtransaction_buffers configuration when set to 0
- Uses a fallback mechanism with PGC_S_OVERRIDE if dynamic default setting fails
- The SUBTRANS system uses SimpleLru for buffer management of subtransaction status pages
- Sets up the SubTransPagePrecedes function for determining page ordering during truncation
- Includes unit tests via SlruPagePrecedesUnitTests to verify page precedence logic
- Located in src/backend/access/transam/subtrans.c:220-253