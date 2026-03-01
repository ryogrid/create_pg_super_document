# Appendix: Glossary

[<< Symbol Index](appendix_symbol_index.md) | [Index](index.md) | [Next: Data Structures >>](appendix_data_structures.md)

---

## Buffer Management Terminology

### Block
An 8 KB unit of data as stored on disk. Identified by a `BlockNumber` (0-based) within a relation fork. On disk, a block is the content of a segment file at a specific offset. When loaded into the buffer pool, it becomes a "page." See also: **Page**.

### Buffer
A slot in the shared buffer pool holding one page of data. Identified by a `Buffer` value (1-based for shared, negative for local, 0 for invalid). Each buffer has a [BufferDesc](03_buffer_pool_architecture.md) descriptor and an 8 KB data block.

### Buffer Descriptor (BufferDesc)
A 64-byte metadata structure associated with each buffer slot. Contains the [BufferTag](#buffertag), an atomic state word (flags + refcount + usage count), a content lock, and freelist linkage. See [Buffer Pool Architecture](03_buffer_pool_architecture.md).

### Buffer Pool
The shared memory region containing all buffer descriptors, data blocks, I/O condition variables, and the hash table. Initialized by [InitBufferPool()](03_buffer_pool_architecture.md). Sized by the `shared_buffers` GUC.

### BufferTag
A 20-byte structure that uniquely identifies a disk block: tablespace OID, database OID, relation file number, fork number, and block number. Used as the hash key for the [buffer lookup table](04_buffer_lookup_and_hashtable.md).

### Cache Hit
When a requested page is already present in the buffer pool. The fast path: shared partition lock + CAS pin. No disk I/O required.

### Cache Miss
When a requested page is not in the buffer pool. Requires victim selection (clock sweep), potential dirty page flush, hash table insertion, and disk I/O.

### Checkpoint
A periodic operation that writes all dirty buffers to disk and records a WAL position (REDO point) from which recovery can begin. See [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md).

### Cleanup Lock
An exclusive content lock combined with a wait for all other backends to unpin the buffer (pin count = 1). Required for operations that physically remove tuples from a page. See [LockBufferForCleanup()](06_page_concurrency_control.md).

### Clock Sweep
The buffer replacement algorithm. A circular scan of buffer descriptors that decrements usage counts and selects victims with refcount = 0 and usage count = 0. See [Buffer Replacement Policy](07_buffer_replacement_policy.md).

### Content Lock
An LWLock embedded in each [BufferDesc](03_buffer_pool_architecture.md) that controls concurrent access to the buffer's page data. Shared mode for reading, exclusive mode for writing. See [Page Concurrency Control](06_page_concurrency_control.md).

### Dirty Buffer
A buffer whose in-memory contents differ from the on-disk version (the `BM_DIRTY` flag is set). Must be flushed before the buffer slot can be reused.

### Double Buffering
The architectural property where data pages exist in both PostgreSQL's buffer pool and the OS kernel page cache. See [Data Movement and Durability](12_data_movement_and_durability.md) and [Deep Dives](15_deep_dives.md).

### Fork
One of several files associated with a relation: MAIN (data), FSM (free space map), VM (visibility map), INIT (for unlogged relations). See [Storage Manager](11_storage_manager.md).

### Free List
A linked list of buffer slots that have never been used or have been explicitly returned. Checked before the clock sweep for fast allocation. See [Buffer Replacement Policy](07_buffer_replacement_policy.md).

### Full-Page Image (FPI)
A complete copy of a page included in a WAL record, used to protect against torn pages. Generated on the first modification after a checkpoint. See [WAL Integration](10_wal_integration.md) and [Deep Dives](15_deep_dives.md).

### Header Spinlock (BM_LOCKED)
A single-bit spinlock in the buffer state word that protects buffer descriptor metadata. Held for only a few instructions. See [Page Concurrency Control](06_page_concurrency_control.md).

### Hint Bit
A status flag on a heap tuple (e.g., `HEAP_XMIN_COMMITTED`) that caches the result of a transaction status lookup. Can be set under only a shared content lock. See [WAL Integration](10_wal_integration.md).

### I/O Lock (BM_IO_IN_PROGRESS)
A flag in the buffer state word that serializes disk I/O on a buffer. Coordinated via per-buffer condition variables. See [Page Concurrency Control](06_page_concurrency_control.md).

### Line Pointer
A 4-byte entry in the page's `pd_linp[]` array that references a tuple's offset and length within the page. Uses 1-based numbering (`OffsetNumber`). See [Page Layout and Types](08_page_layout_and_types.md).

### Local Buffer
A backend-private buffer used for temporary table pages. No shared memory, no locking, no WAL. See [Local Buffers](13_local_buffers.md).

### LSN (Log Sequence Number)
A 64-bit value identifying a position in the WAL stream. Stored in each page header as `pd_lsn`. Used to enforce the [WAL-before-data rule](10_wal_integration.md).

### Page
An 8 KB unit of data as it exists in the buffer pool (in memory). Structured with a [PageHeaderData](08_page_layout_and_types.md) header, line pointers, tuple data, and optional special space. When written to disk, it becomes a "block." In practice, the terms are often used interchangeably.

### Partition Lock
One of 128 LWLocks protecting segments of the [buffer hash table](04_buffer_lookup_and_hashtable.md). Shared mode for lookups, exclusive mode for insertions and deletions.

### Pin (Buffer Pin)
A reference count on a buffer that prevents eviction. Implemented as bits 0-17 of the atomic state word. Each backend also maintains a private refcount. See [Page Concurrency Control](06_page_concurrency_control.md).

### Relation
A PostgreSQL table or index. In the context of buffer management, relations are identified by `RelFileLocator` (tablespace, database, relfilenode) rather than by name or OID.

### Ring Buffer
A backend-private circular buffer used by bulk operations to avoid polluting the shared buffer pool. See [Buffer Replacement Policy](07_buffer_replacement_policy.md).

### Segment
A 1 GB file chunk of a relation fork. The MD layer splits large relations into segments to avoid platform file-size limits. See [Storage Manager](11_storage_manager.md).

### Special Space
An optional region at the end of a page used by access methods for opaque data (e.g., btree's `BTPageOpaqueData`). Delimited by `pd_special`. See [Page Layout and Types](08_page_layout_and_types.md).

### Usage Count
A 4-bit counter (range 0-5) in the buffer state word that approximates access frequency. Incremented on pin, decremented by clock sweep. See [Buffer Replacement Policy](07_buffer_replacement_policy.md).

### VFD (Virtual File Descriptor)
An abstraction layer that manages OS file descriptors with LRU recycling, allowing PostgreSQL to access more files than the OS permits open simultaneously. See [Storage Manager](11_storage_manager.md).

### Victim Buffer
A buffer selected by the clock sweep for eviction and reuse. Must have refcount = 0 and usage count = 0. If dirty, it is flushed to disk before reuse.

### WAL-Before-Data
The fundamental rule: a dirty page cannot be written to disk until the WAL record describing the change has been flushed. Enforced in [FlushBuffer()](09_dirty_buffer_and_writeback.md) via [XLogFlush()](10_wal_integration.md).

### Writeback Advisory
A mechanism that advises the OS to asynchronously flush dirty kernel pages to disk, smoothing I/O patterns between writes and checkpoint fsync. See [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md).

---

[<< Symbol Index](appendix_symbol_index.md) | [Index](index.md) | [Next: Data Structures >>](appendix_data_structures.md)
