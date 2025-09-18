# PageSetChecksumInplace

## Location
[src/backend/storage/page/bufpage.c:1542-1549](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L1542-L1549)

## Overview
Calculates and sets the checksum directly on a page in private memory, used when concurrent modifications are guaranteed not to occur.

## Definition


## Detailed Description
PageSetChecksumInplace provides an efficient checksum calculation mechanism for pages that reside in private memory where concurrent modifications are impossible. Unlike PageSetChecksumCopy, this function modifies the page directly without creating a copy, making it suitable for scenarios where:

1. **Private memory**: The page is in process-private memory, not shared buffers
2. **Single-threaded access**: No concurrent processes can modify the page
3. **Performance optimization**: Avoids the overhead of memory copying

The function is a lightweight wrapper that performs early validation checks and directly updates the page's checksum field. It's commonly used during buffer flushing operations where exclusive access is already guaranteed, and in bulk operations where pages are being prepared for writing.

## Parameters / Member Variables
- : The page to checksum in-place (must be in private memory)
- : The block number of the page, used in checksum calculation

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](PageIsNew.md)
  - [DataChecksumsEnabled](../D/DataChecksumsEnabled.md)
  - [pg_checksum_page](../p/pg_checksum_page.md)
- Called from (representative examples):
  - [_hash_alloc_buckets](../h/_hash_alloc_buckets.md) (Hash index bucket allocation)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md) (Relation buffer flushing)
  - GetLocalVictimBuffer (Local buffer management)
  - [smgr_bulk_flush](../s/smgr_bulk_flush.md) (Bulk storage manager flush operations)

## Notes and Other Information
- **Critical safety requirement**: Only use when no concurrent modifications possible
- More efficient than PageSetChecksumCopy due to no memory copying overhead
- Returns immediately if checksums are disabled or page is uninitialized
- Directly modifies the pd_checksum field in the page header
- Essential for performance in bulk operations and private buffer scenarios
- Commonly used in storage manager operations and local buffer management
- Simpler implementation compared to PageSetChecksumCopy but with stricter usage constraints
- Part of PostgreSQL's dual-strategy approach to safe checksum calculation