# Appendix: GUC Parameters

[<< Data Structures](appendix_data_structures.md) | [Index](index.md) | [Next: Quick Reference >>](buffer_mgmt_quick_reference.md)

---

## Buffer Pool Sizing

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `shared_buffers` | 128 MB | Size of the shared buffer pool. Primary tuning knob. Each buffer is 8 KB (BLCKSZ). Typical production: 25-40% of system RAM. | [Buffer Pool Architecture](03_buffer_pool_architecture.md) |
| `num_temp_buffers` | 8 MB | Size of each backend's local buffer pool for temporary tables. Default = 1024 buffers. | [Local Buffers](13_local_buffers.md) |

## Background Writer

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `bgwriter_delay` | 200 ms | Time between background writer rounds. Lower values = more responsive but higher CPU usage. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `bgwriter_lru_maxpages` | 100 | Maximum number of buffers the bgwriter can write per round. 0 disables bgwriter cleaning. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `bgwriter_lru_multiplier` | 2.0 | Safety margin multiplier for the estimated buffer demand. Higher values = more proactive cleaning. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `bgwriter_flush_after` | 512 KB | Writeback advisory coalescing limit for the background writer. After writing this many bytes, issue a writeback advisory. 0 disables. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

## Checkpoint

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `checkpoint_timeout` | 5 min | Maximum time between automatic checkpoints. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `checkpoint_completion_target` | 0.9 | Fraction of checkpoint interval over which to spread buffer writes. Higher values = smoother I/O but potentially longer recovery. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `checkpoint_flush_after` | 256 KB | Writeback advisory coalescing limit for the checkpointer. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `checkpoint_warning` | 30 s | Log a warning if checkpoints are triggered by WAL volume more frequently than this interval. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |
| `max_wal_size` | 1 GB | Approximate maximum WAL size between checkpoints. Larger values allow longer checkpoint intervals but increase recovery time. | [WAL Integration](10_wal_integration.md) |
| `min_wal_size` | 80 MB | Minimum WAL size to retain. | [WAL Integration](10_wal_integration.md) |

## I/O Concurrency and Prefetch

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `effective_io_concurrency` | 1 | Number of concurrent I/O operations for bitmap heap scans. For SSDs: 10-200. | [Access Method Integration](14_access_method_integration.md) |
| `maintenance_io_concurrency` | 10 | I/O concurrency for maintenance operations (VACUUM, CREATE INDEX). | [Access Method Integration](14_access_method_integration.md) |
| `io_combine_limit` | 128 KB | Maximum amount of data to combine into a single I/O operation. | [Buffer Access Protocol](05_buffer_access_protocol.md) |

## WAL and Durability

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `wal_level` | replica | WAL detail level. Affects full-page write frequency. | [WAL Integration](10_wal_integration.md) |
| `full_page_writes` | on | Enable full-page writes for torn page protection. Disabling risks corruption after crash. | [WAL Integration](10_wal_integration.md), [Deep Dives](15_deep_dives.md) |
| `wal_compression` | off | Compress full-page images in WAL. Reduces WAL volume by 50-80% at small CPU cost. | [Deep Dives](15_deep_dives.md) |
| `wal_buffers` | -1 (auto) | Shared memory for WAL buffers. Auto-tuned based on shared_buffers. | [WAL Integration](10_wal_integration.md) |

## Direct I/O

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `io_direct` | '' (empty) | Bypass OS page cache. Values: 'data', 'wal', 'data,wal'. Eliminates double buffering. | [Data Movement and Durability](12_data_movement_and_durability.md) |

## Backend Writeback

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `backend_flush_after` | 0 (disabled) | Writeback advisory coalescing limit for backend processes. When non-zero, backends issue writeback advisories after flushing this many bytes during eviction. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

## Data Integrity

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `data_checksums` | off | Enable per-page checksums. Must be set at initdb time. When enabled, hint bit updates require WAL logging. | [Page Layout and Types](08_page_layout_and_types.md), [Deep Dives](15_deep_dives.md) |
| `ignore_checksum_failure` | off | Continue despite checksum failures (for emergency recovery). | [Page Layout and Types](08_page_layout_and_types.md) |
| `zero_damaged_pages` | off | Zero out pages that fail verification instead of raising an error. | [Storage Manager](11_storage_manager.md) |

## VACUUM Buffer Usage

| Parameter | Default | Description | Documented In |
|-----------|---------|-------------|---------------|
| `vacuum_buffer_usage_limit` | 2 MB | Size of the BAS_VACUUM ring buffer. Controls how much of the shared buffer pool VACUUM can use. | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `vacuum_cost_page_hit` | 1 | Cost of accessing a page in the buffer pool during VACUUM. | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `vacuum_cost_page_miss` | 2 | Cost of reading a page from disk during VACUUM. | [Buffer Replacement Policy](07_buffer_replacement_policy.md) |
| `vacuum_cost_page_dirty` | 20 | Cost of dirtying a clean page during VACUUM. | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) |

---

[<< Data Structures](appendix_data_structures.md) | [Index](index.md) | [Next: Quick Reference >>](buffer_mgmt_quick_reference.md)
