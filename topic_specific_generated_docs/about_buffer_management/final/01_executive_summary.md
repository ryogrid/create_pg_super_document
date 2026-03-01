# Executive Summary: PostgreSQL Buffer Management

[<< Index](index.md) | [Next: Architecture Overview >>](02_architecture_overview.md)

---

## What Is the Buffer Manager?

The buffer manager is the layer that mediates **all** data access between PostgreSQL backends and the disk. Every read or write of a relation page (heap tuple, index entry, free space map, visibility map) passes through it. The buffer manager maintains a pool of 8 KB page slots in shared memory, implements a replacement algorithm, enforces concurrency control, and guarantees WAL-before-data for crash recovery.

## Core Architecture in One Diagram

```
  SQL Query
     |
  Access Method (heap, btree, ...)
     |
  Buffer Manager ---- ReadBuffer() / MarkBufferDirty() / ReleaseBuffer()
     |
     +-- Hash Table: BufferTag -> buf_id (128 partitions)
     +-- Buffer Descriptors: metadata array (64 bytes each)
     +-- Buffer Blocks: page data array (8 KB each)
     +-- Clock Sweep: replacement policy
     |
  Storage Manager (smgr -> md.c -> VFD -> OS)
     |
  Kernel Page Cache
     |
  Disk
```

## Key Design Decisions

1. **Explicit buffer pool** rather than relying on the OS page cache, enabling pin-based concurrency, WAL integration, and database-tuned replacement.

2. **Clock sweep replacement** with usage counts (0-5) approximates LRU with minimal overhead -- one atomic counter for the clock hand.

3. **Ring buffers** for bulk operations (sequential scans, VACUUM, COPY) confine large scans to a small ring, protecting hot pages.

4. **Lock-free pinning** via CAS on a packed 32-bit state word (flags + usage count + refcount) avoids spinlocks on the common read path.

5. **WAL-before-data** enforced in `FlushBuffer()`: dirty pages cannot reach disk until their WAL records are flushed.

## Critical Data Structures

| Structure | Purpose | Source |
|-----------|---------|--------|
| [BufferDesc](03_buffer_pool_architecture.md) | Per-buffer metadata: tag, state, content lock | `src/include/storage/buf_internals.h` |
| [BufferTag](03_buffer_pool_architecture.md) | Page identity: tablespace + db + relation + fork + block | `src/include/storage/buf_internals.h` |
| [PageHeaderData](08_page_layout_and_types.md) | Page header: LSN, checksum, free space pointers | `src/include/storage/bufpage.h` |
| [BufferStrategyControl](07_buffer_replacement_policy.md) | Clock sweep state, free list, bgwriter coordination | `src/backend/storage/buffer/freelist.c` |
| [SMgrRelationData](11_storage_manager.md) | Per-relation file handle cache | `src/include/storage/smgr.h` |

## Critical Code Paths

| Operation | Key Function | What It Does |
|-----------|-------------|--------------|
| Read a page | [ReadBuffer()](05_buffer_access_protocol.md) | Hash lookup, pin, disk I/O if miss |
| Allocate a buffer | [BufferAlloc()](05_buffer_access_protocol.md) | Hash insert, victim selection, tag assignment |
| Find a victim | [StrategyGetBuffer()](07_buffer_replacement_policy.md) | Free list, then clock sweep |
| Mark page modified | [MarkBufferDirty()](09_dirty_buffer_and_writeback.md) | CAS to set BM_DIRTY |
| Write page to disk | [FlushBuffer()](09_dirty_buffer_and_writeback.md) | WAL flush, checksum, smgrwrite |
| Background cleaning | [BgBufferSync()](09_dirty_buffer_and_writeback.md) | Adaptive pre-cleaning ahead of clock sweep |
| Checkpoint flush | [BufferSync()](09_dirty_buffer_and_writeback.md) | Scan all buffers, sort by tablespace, write |
| Content locking | [LockBuffer()](06_page_concurrency_control.md) | LWLock shared/exclusive on buffer data |
| Cleanup lock | [LockBufferForCleanup()](06_page_concurrency_control.md) | Exclusive + wait for all pins to drain |

## Configuration Knobs

| Parameter | Default | Impact |
|-----------|---------|--------|
| `shared_buffers` | 128 MB | Buffer pool size -- primary tuning knob |
| `bgwriter_delay` | 200 ms | Background writer wakeup interval |
| `bgwriter_lru_multiplier` | 2.0 | Safety margin for pre-cleaning estimate |
| `checkpoint_completion_target` | 0.9 | Spread checkpoint writes over this fraction of interval |
| `effective_io_concurrency` | 1 | Prefetch depth for bitmap scans |

See [GUC Parameters](appendix_guc_parameters.md) for the complete list.

## Where to Read Next

- **Understand the architecture**: [Architecture Overview](02_architecture_overview.md)
- **Dive into the code**: [Buffer Pool Architecture](03_buffer_pool_architecture.md)
- **Learn the read path**: [Buffer Access Protocol](05_buffer_access_protocol.md)
- **Tune performance**: [Buffer Replacement Policy](07_buffer_replacement_policy.md) and [GUC Parameters](appendix_guc_parameters.md)

---

[<< Index](index.md) | [Next: Architecture Overview >>](02_architecture_overview.md)
