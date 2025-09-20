# ReorderBufferCheckMemoryLimit

## Location
[src/backend/replication/logical/reorderbuffer.c:3767-3839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3767-L3839)

## Overview
Monitors the logical decoding memory limit and triggers eviction of the largest transactions through streaming or disk serialization to maintain memory usage below configured thresholds.

## Definition

```c
static void
ReorderBufferCheckMemoryLimit(ReorderBuffer *rb)
```
## Detailed Description
ReorderBufferCheckMemoryLimit is the core memory management function for PostgreSQL's logical replication system. It monitors the reorder buffer's memory usage against the  limit and implements a two-tier eviction strategy when memory pressure is detected.

The function operates in different modes based on the  configuration:

**Buffered Mode (normal operation):**
- Only activates when memory usage exceeds 
- Evicts transactions until memory usage drops below the limit

**Immediate Mode (debugging/testing):**
- Forces eviction of all transactions regardless of memory usage
- Continues until the buffer is completely empty

**Eviction Strategy:**
1. **Streaming Preferred:** If streaming is enabled () and a suitable streamable top-level transaction exists, it streams the largest streamable transaction using 
2. **Serialization Fallback:** Otherwise, it serializes the largest transaction (including subtransactions) to disk using 

The function includes extensive validation through assertions to ensure data consistency throughout the eviction process and guarantees that memory usage is below the limit when it completes.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer structure to check and potentially evict transactions from

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferCanStartStreaming](ReorderBufferCanStartStreaming.md) (checks if streaming is enabled and available)
  - [ReorderBufferLargestStreamableTopTXN](ReorderBufferLargestStreamableTopTXN.md) (finds largest streamable top-level transaction)
  - [ReorderBufferLargestTXN](ReorderBufferLargestTXN.md) (finds largest transaction including subtransactions)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md) (streams a transaction to output plugin)
  - [ReorderBufferSerializeTXN](ReorderBufferSerializeTXN.md) (serializes a transaction to disk)
  - rbtxn_is_toptxn (verifies transaction is a top-level transaction)
- Called from (representative examples):
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md) (after adding changes to monitor memory usage)
  - Various transaction processing points to maintain memory limits

## Notes and Other Information
- This is a static function, only accessible within reorderbuffer.c
- Central component of PostgreSQL's logical replication memory management
- Implements a greedy eviction strategy (always evicts the largest transaction)
- Future enhancement opportunity: could implement more sophisticated eviction strategies (e.g., freeing a percentage of memory limit)
- Handles dynamic memory limit changes gracefully (user can reduce  during operation)
- Extensive use of assertions for debugging and data consistency verification
- The  parameter provides testing and debugging capabilities
- Memory limit is specified in KB, converted to bytes by multiplying by 1024L