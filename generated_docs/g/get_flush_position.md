# get_flush_position

## Location
[src/backend/replication/logical/worker.c:3405-3448](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3405-L3448)

## Overview
Determines the appropriate write and flush LSN positions to report back to the walsender in logical replication by correlating local and remote LSN mappings.

## Definition

```c
static void
get_flush_position(XLogRecPtr *write, XLogRecPtr *flush,
				   bool *have_pending_txes)
```
## Detailed Description
This function implements a critical component of PostgreSQL's logical replication feedback mechanism. It solves the fundamental challenge that the subscriber cannot simply report the last LSN received from the publisher because local transactions might not yet be flushed to disk locally.

The function works by:

1. **LSN Mapping Management**: Maintains a list (lsn_mapping) that associates local LSNs with remote LSNs for each committed transaction
2. **Local Flush Detection**: Uses GetFlushRecPtr() to determine what has been flushed locally to disk  
3. **Safe Reporting**: Iterates through the mapping list to find which remote LSNs can be safely reported as flushed
4. **Efficient Processing**: 
   - Removes flushed entries from the mapping list to avoid reprocessing
   - Optimizes by jumping to the tail when encountering unflushed entries
   - Sets the write position to the last remote LSN in the mapping

5. **Pending Transaction Tracking**: Indicates whether there are outstanding transactions awaiting local flush

The algorithm ensures that only LSNs corresponding to locally-flushed data are reported as flushed to the publisher, maintaining data consistency and preventing premature acknowledgments that could lead to data loss.

## Parameters / Member Variables
- : Output parameter for the write position (remote LSN) that can be reported to walsender
- : Output parameter for the flush position (remote LSN) that can be safely reported as flushed  
- : Output parameter indicating if there are transactions not yet flushed locally

## Dependencies
- Functions called/Symbols referenced:
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - dlist_foreach_modify
  - dlist_container
  - [dlist_delete](../d/dlist_delete.md)
  - dlist_tail_element
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [send_feedback](../s/send_feedback.md)

## Notes and Other Information
- Uses a doubly-linked list (lsn_mapping) to maintain LSN associations in commit order
- Critical for maintaining data consistency in logical replication by preventing premature flush acknowledgments  
- The function modifies the lsn_mapping list by removing entries that have been safely flushed
- Optimized to avoid unnecessary iteration over potentially long lists of unflushed transactions
- InvalidXLogRecPtr is used as the initial value for write and flush positions before processing
- The have_pending_txes flag helps the caller understand replication lag status