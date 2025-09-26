# brin_initialize_empty_new_buffer

## Location
[src/backend/access/brin/brin_pageops.c:884-915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L884-L915)

## Overview
Initializes a buffer as an empty regular BRIN index page, logs the initialization for crash recovery, and records the page's free space in the FSM.

## Definition
```c
static void brin_initialize_empty_new_buffer(Relation idxrel, Buffer buffer)
```

## Detailed Description
This function performs the complete initialization of a new BRIN index page, ensuring that it is properly set up for use, durably logged for crash recovery, and registered in the Free Space Map for future space management. It addresses several important corner cases where pages are extended but cannot be immediately used.

The initialization process consists of several critical steps:

1. **Page Initialization**: Uses brin_page_init to set up the page structure as a regular BRIN page
2. **WAL Logging**: Creates a full-page image in the WAL using log_newpage_buffer to ensure crash recovery
3. **FSM Registration**: Records the page's available free space in the Free Space Map for future allocations

The function is designed to handle scenarios where the relation is extended to obtain a new page, but circumstances prevent its immediate use. Without proper initialization and recording, such pages would become unusable space that could cause index bloat.

The critical section ensures atomicity of the page initialization and WAL logging operations. The FSM update is performed outside the critical section since it's not WAL-logged, but this is acceptable because VACUUM operations will eventually correct any inconsistencies.

## Parameters / Member Variables
- `idxrel`: Relation structure representing the BRIN index
- `buffer`: Buffer containing the new page to initialize, must be exclusively locked

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (to access the page from buffer)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md) (to get the page's block number for logging and FSM)
  - [brin_page_init](brin_page_init.md) (to initialize the page structure)
  - [MarkBufferDirty](../M/MarkBufferDirty.md) (to mark the buffer as modified)
  - [log_newpage_buffer](../l/log_newpage_buffer.md) (to create a WAL record with full page image)
  - [RecordPageWithFreeSpace](../R/RecordPageWithFreeSpace.md) (to register the page in FSM)
  - [br_page_get_freespace](br_page_get_freespace.md) (to calculate available free space)
  - START_CRIT_SECTION/END_CRIT_SECTION (for critical section management)
  - BRIN_PAGETYPE_REGULAR (page type constant)
  - BRIN_elog, DEBUG2 (for debug logging)
- Called from:
  - [brin_doupdate](brin_doupdate.md) (when initializing pages during tuple updates)
  - [brin_page_cleanup](brin_page_cleanup.md) (when handling uninitialized pages during vacuum)
  - [brin_getinsertbuffer](brin_getinsertbuffer.md) (when extending the relation for new insertions)

## Notes and Other Information
- This is a static function internal to brin_pageops.c
- The function ensures that all page initialization is properly WAL-logged for crash recovery and standby replication
- FSM updates are not WAL-logged, but VACUUM operations will correct any inconsistencies after crashes
- Uses critical sections to ensure atomicity of the initialization and logging operations
- Part of the BRIN index space management infrastructure that prevents index bloat from unusable pages
- The caller is responsible for updating upper FSM pages if appropriate
- Essential for maintaining index consistency across crashes and in standby servers
- The function specifically addresses corner cases where pages are extended but cannot be immediately utilized
- Debug logging helps track page initialization for troubleshooting and monitoring purposes