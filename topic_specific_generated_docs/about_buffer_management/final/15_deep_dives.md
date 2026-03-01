# Deep Dives

[<< Access Method Integration](14_access_method_integration.md) | [Index](index.md) | [Next: Symbol Index >>](appendix_symbol_index.md)

---

## 1. Double Buffering Analysis

PostgreSQL maintains its own buffer pool on top of the OS kernel page cache, creating a situation where the same data page can exist in memory twice. This section analyzes the costs, benefits, and mitigation strategies.

### Why Double Buffering Exists

PostgreSQL cannot rely solely on the OS page cache because:
- The OS has no concept of [buffer pins](06_page_concurrency_control.md) -- it could evict a page a backend is actively reading.
- The OS cannot enforce the [WAL-before-data rule](10_wal_integration.md).
- The OS's generic LRU policy is not tuned for database workloads ([clock sweep](07_buffer_replacement_policy.md) with usage counts is superior for mixed read patterns).
- Checkpoint coordination requires knowing which pages are dirty.

### Memory Cost

For a typical production configuration with `shared_buffers = 8 GB`, the OS may cache another 8-16 GB of the same relation data. This duplication reduces the effective memory available for caching. The waste is proportional to the working set overlap between PostgreSQL's buffer pool and the OS cache.

### Mitigation: io_direct

PostgreSQL 16+ provides `io_direct = 'data'` to bypass the OS page cache entirely, using `O_DIRECT` for data file I/O. See [Data Movement and Durability](12_data_movement_and_durability.md). This eliminates double buffering but removes OS-level read-ahead, which can hurt sequential scan performance.

### Mitigation: Sizing shared_buffers

The common recommendation is to set `shared_buffers` to 25-40% of total RAM, leaving the rest for the OS page cache (which still provides value for OS-level read-ahead and for data not yet accessed through PostgreSQL's buffer pool).

## 2. Full-Page Writes (FPW) Deep Dive

### The Torn Page Problem

Standard operating systems do not guarantee atomic 8 KB writes. If a crash occurs mid-write, the page on disk may contain a mix of old and new data -- a "torn page." Since WAL replay applies incremental changes to existing pages, a torn base page makes the incremental record unrecoverable.

### How FPW Solves It

The first time a page is modified after a checkpoint, the complete 8 KB page image is included in the WAL record (a "full-page image" or FPI). During recovery:

1. If the WAL record has an FPI, it replaces the on-disk page entirely -- torn pages are overwritten.
2. Subsequent WAL records for the same page (before the next checkpoint) apply incremental changes to the restored page.

### Performance Impact

FPW significantly increases WAL volume, especially right after a checkpoint when many pages receive their first modification. The increase depends on workload:

- **OLTP with small updates**: WAL volume may increase 2-4x during the initial phase after checkpoint.
- **Bulk loads**: Less impact since pages are written sequentially and fewer FPIs are needed.

Mitigation: `wal_compression = on` compresses FPIs in WAL records, reducing volume by 50-80% at a small CPU cost.

### Interaction with Checksums

When data checksums are enabled, [hint bit updates](10_wal_integration.md) must also generate FPIs via `XLogSaveBufferForHint()`. This is because:
1. A hint bit update changes the page data (invalidating the current checksum).
2. If a crash occurs during the write, the on-disk page could have correct data but an incorrect checksum.
3. On recovery, this would be detected as corruption.

The FPI ensures that after recovery, both the data and the checksum are consistent.

## 3. Checksum Handling Deep Dive

### The PageSetChecksumCopy() Design

[FlushBuffer()](09_dirty_buffer_and_writeback.md) holds only a shared content lock, which allows concurrent hint bit updates. If `FlushBuffer()` computed the checksum in-place on the live page, a concurrent hint bit update could:

1. Modify a byte in the page after the checksum includes it but before the write completes.
2. The on-disk page would have a mismatched checksum.

Solution: `PageSetChecksumCopy()` copies the entire 8 KB page to a static buffer and computes the checksum on the copy. The copy is atomically consistent because it is created by `memcpy()` while the shared content lock prevents structural modifications (only hint bits can change).

### Why This Is Safe

Hint bit updates under shared lock are specifically designed to be safe for concurrent reads:
- They only set bits (never clear them).
- They do not change page structure (pd_lower, pd_upper, line pointers).
- The copy-on-write checksum handles the potential for partially-updated hint bits.

### PageSetChecksumInplace() Alternative

When the caller holds an exclusive content lock (e.g., during recovery replay or explicit page rewrites), `PageSetChecksumInplace()` can set the checksum directly without copying.

## 4. Lock Contention Analysis

### Buffer Mapping Partition Lock Contention

The 128-partition [hash table](04_buffer_lookup_and_hashtable.md) distributes contention well for typical workloads. However, under extreme concurrency (hundreds of backends accessing a small number of hot pages), partition lock contention can become visible.

**Diagnosis:** `pg_stat_activity` wait events showing `LWLock:BufferMapping` indicate partition lock contention.

**Mitigation:**
- Increase `shared_buffers` to reduce cache miss rate (fewer exclusive locks for insertion).
- Use `pg_buffercache` to identify hot pages and verify they are cached.
- Reduce lock hold time by ensuring no unnecessary work occurs between lock acquisition and release (the common hit path is already optimized for this).

### Buffer Content Lock Contention

Content lock contention manifests as `LWLock:BufferContent` wait events. Common scenarios:
- **High-concurrency updates to the same page**: Multiple backends updating different rows on the same page. Consider `fillfactor` to spread updates across more pages.
- **Concurrent sequential scans + updates**: Readers hold shared locks that block writers. This is by design and rarely problematic.

### Header Spinlock Contention

The buffer header spinlock (`BM_LOCKED`) is extremely short-lived. Contention is rare and typically indicates:
- An extremely high rate of pin/unpin on the same buffer from different CPUs.
- Consider whether the workload can be restructured to reduce hot-spot access.

## 5. Crash Recovery and Buffer Management

### Recovery Mode Buffer Access

During crash recovery, the startup process replays WAL records and modifies buffer pages:

1. **ReadBufferWithoutRelcache()**: Reads pages without requiring a relation cache entry (the relation may not yet exist in the catalog during early recovery).
2. **XLogInitBufferForRedo()**: The primary interface for recovery replay:
   - If the WAL record contains an FPI, restores the page from the image.
   - If no FPI, reads the current on-disk page and verifies the LSN to determine if the change has already been applied.
3. **MarkBufferDirty()**: After applying the WAL record, the buffer is marked dirty.
4. The modified pages are eventually written back via checkpoint or bgwriter, just like in normal operation.

### LSN-Based Idempotency

WAL replay is idempotent because of LSN comparison:
- If the page's LSN is already >= the WAL record's LSN, the change has already been applied and is skipped.
- This handles the case where a page was written to disk after the WAL record but before the crash.

### Recovery Buffer Pool Behavior

During recovery, the buffer pool operates similarly to normal mode but with some differences:
- No concurrent writers (only the startup process modifies pages).
- The [background writer](09_dirty_buffer_and_writeback.md) runs and proactively cleans dirty buffers.
- [Checkpoints](09_dirty_buffer_and_writeback.md) during recovery (restartpoints) flush dirty buffers to reduce recovery time on subsequent crashes.

## 6. Ring Buffer Strategy Deep Dive

### Why Ring Buffers Exist

Without ring buffers, a sequential scan of a large table would cycle through the entire buffer pool, evicting all cached hot pages. This is catastrophic for concurrent OLTP workloads.

### BAS_BULKREAD (256 KB Ring)

The small 32-buffer ring confines sequential scans to a minimal memory footprint. The size is chosen to fit in L2 cache, optimizing the transfer from OS cache to PostgreSQL buffers.

**WAL rejection behavior**: If a ring buffer is dirty and requires a WAL flush to write back, the buffer is removed from the ring rather than forcing the scan to wait for WAL I/O. This is unique to `BAS_BULKREAD` because sequential scans are read-only operations that should not pay WAL flush costs.

### BAS_BULKWRITE (16 MB Ring)

COPY IN and CREATE TABLE AS use a larger ring because:
- They generate many dirty pages that must be flushed.
- A larger ring amortizes WAL flush overhead over more pages.
- The 16 MB size ensures the writeback advisory can coalesce writes effectively.

### BAS_VACUUM (2 MB Ring)

VACUUM uses a medium ring (configurable via `vacuum_buffer_usage_limit`) because:
- It reads and writes pages (dirty pages from pruning).
- The ring must be large enough to avoid frequent WAL flushes.
- But small enough not to displace too many hot pages.

### Ring Size Cap

All ring sizes are capped at `NBuffers / 8` (12.5% of the buffer pool). This prevents any single bulk operation from consuming more than an eighth of the shared buffers, even on systems with small `shared_buffers`.

## 7. Background Writer Tuning

### Understanding the Adaptive Algorithm

[BgBufferSync()](09_dirty_buffer_and_writeback.md) tracks two moving averages:

- **smoothed_alloc**: The rate at which backends are consuming clean buffers. Fast attack (immediately follows increases) to quickly respond to load spikes, slow decay to avoid premature hibernation.
- **smoothed_density**: The density of reusable buffers in the scanned range (inverse of how far the clock sweep must advance per allocation).

The product `smoothed_alloc * smoothed_density * bgwriter_lru_multiplier` gives the number of buffers the bgwriter should scan ahead. The `bgwriter_lru_multiplier` (default 2.0) provides a safety margin.

### When to Increase bgwriter_lru_maxpages

If `buffers_backend` in `pg_stat_bgwriter` (now `pg_stat_io`) is consistently high relative to `buffers_clean`, backends are frequently performing synchronous writes during eviction. Increasing `bgwriter_lru_maxpages` allows the bgwriter to clean more buffers per round.

### When to Decrease bgwriter_delay

Reducing `bgwriter_delay` (default 200ms) makes the bgwriter respond faster to load changes but increases CPU usage during idle periods. Consider reducing it for latency-sensitive workloads with bursty write patterns.

---

[<< Access Method Integration](14_access_method_integration.md) | [Index](index.md) | [Next: Symbol Index >>](appendix_symbol_index.md)
