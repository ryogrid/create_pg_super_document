# Appendix — GUC Parameters

[Up: index.md](index.md)  |  [Prev: appendix_pgdata_layout.md](appendix_pgdata_layout.md)  |  [Next: metadata_quick_reference.md](metadata_quick_reference.md)

Every GUC parameter that materially affects metadata behavior, with a
short description and a pointer to the chapter that explains the
mechanism.

## Cluster-wide policy

| GUC                                | Default          | Touched in                | Effect                                                                         |
|------------------------------------|------------------|---------------------------|--------------------------------------------------------------------------------|
| `track_commit_timestamp`           | `off`            | [11](11_commit_timestamps.md) | Activate/deactivate the CommitTs SLRU. Off → `TransactionIdGetCommitTsData` always returns false. |
| `wal_log_hints`                    | `off`            | [13](13_visibility_map.md), [21 §14](21_deep_dives.md) | Force `XLOG_FPI_FOR_HINT` emission for hint-bit-only changes. Required for crash safety under data checksums. |
| `data_checksums`                   | (initdb option)  | [21 §14](21_deep_dives.md) | Per-page CRC; non-mutable post-initdb (`pg_checksums --enable` requires offline cluster). |
| `wal_level`                        | `replica`        | [03](03_catalog_data_model_and_bootstrap.md), [16](16_checkpoints_and_recovery.md) | `minimal` / `replica` / `logical`. Controls how many extra WAL records (and what kind) get emitted. Recorded in `CheckPoint::wal_level`. |
| `max_wal_senders`                  | `10`             | [03](03_catalog_data_model_and_bootstrap.md) | Hot-standby slot count; recorded in pg_control. |
| `hot_standby`                      | `on`             | [16](16_checkpoints_and_recovery.md) | Allow read queries during WAL replay; affects standby startup behaviour. |

## Recovery targets (PITR)

| GUC                          | Used by                                  | Effect                                                                         |
|------------------------------|------------------------------------------|--------------------------------------------------------------------------------|
| `recovery_target`            | `StartupXLOG`                            | `'immediate'`: stop after first consistent point.                              |
| `recovery_target_name`       | `StartupXLOG`                            | Stop after the named restore-point WAL record.                                  |
| `recovery_target_time`       | `StartupXLOG`                            | Stop at the first commit ≤ given timestamp.                                     |
| `recovery_target_xid`        | `StartupXLOG`                            | Stop at the named XID.                                                          |
| `recovery_target_lsn`        | `StartupXLOG`                            | Stop at the named LSN.                                                          |
| `recovery_target_timeline`   | `StartupXLOG`                            | Which timeline to follow.                                                       |
| `recovery_target_inclusive`  | `StartupXLOG`                            | Whether the target itself is replayed.                                          |
| `recovery_target_action`     | `StartupXLOG`                            | `pause` (default), `promote`, `shutdown`.                                       |

## CLOG, MultiXact, CommitTs, SUBTRANS — wraparound

| GUC                                       | Touched in       | Effect                                                                         |
|-------------------------------------------|------------------|--------------------------------------------------------------------------------|
| `autovacuum_freeze_max_age`               | [09](09_clog.md), [21 §15](21_deep_dives.md) | XID-age threshold beyond which autovacuum forces aggressive freeze.            |
| `vacuum_freeze_table_age`                 | [09](09_clog.md) | Per-table XID-age threshold for full-table vacuum scan.                         |
| `vacuum_freeze_min_age`                   | [09](09_clog.md) | Minimum XID-age for tuples to be eligible for freezing.                         |
| `autovacuum_multixact_freeze_max_age`     | [12](12_multixact.md), [21 §15](21_deep_dives.md) | MultiXactId-age threshold; same role as `autovacuum_freeze_max_age` but for multis. |
| `vacuum_multixact_freeze_table_age`       | [12](12_multixact.md) | Per-table multi-age threshold.                                                  |
| `vacuum_multixact_freeze_min_age`         | [12](12_multixact.md) | Minimum multi-age for tuples to be eligible for multi-replacement.              |

## SLRU buffer sizing

| GUC                                | Default | Affects SLRU              | Notes                                              |
|------------------------------------|---------|---------------------------|----------------------------------------------------|
| `transaction_buffers`              | `-1`    | CLOG (XactCtl)            | -1 = auto-tune from `MaxBackends`.                  |
| `subtransaction_buffers`           | `-1`    | SUBTRANS                  | -1 = auto-tune.                                     |
| `multixact_offset_buffers`         | `-1`    | MultiXact offsets         | -1 = auto-tune.                                     |
| `multixact_member_buffers`         | `-1`    | MultiXact members         | -1 = auto-tune.                                     |
| `commit_timestamp_buffers`         | `-1`    | CommitTs                  | -1 = auto-tune.                                     |
| `notify_buffers`                   | `16`    | Notify                    | Sized for typical NOTIFY traffic.                   |
| `serializable_buffers`             | `32`    | Serial (SSI)              | -                                                  |

`SimpleLruAutotuneBuffers()` (slru.c) computes the auto-tuned size as
a function of `MaxBackends` and `BLCKSZ`, capped at
`SLRU_MAX_ALLOWED_BUFFERS` (1 GiB / BLCKSZ).

## Catalog cache and statistics

| GUC                                | Default          | Touched in       | Effect                                                                         |
|------------------------------------|------------------|------------------|--------------------------------------------------------------------------------|
| `default_statistics_target`        | `100`            | [18](18_catalog_inventory.md) §statistics | Per-column histogram bucket count for ANALYZE. |
| `track_io_timing`                  | `off`            | (stats collector) | Whether to record I/O timing per backend.                                      |
| `huge_pages`                       | `try`            | (shmem layout)    | Use OS-level huge pages for shared memory (relcache, catcache, SLRU all live there). |

## Vacuum and freeze

| GUC                                | Default          | Effect                                                                         |
|------------------------------------|------------------|--------------------------------------------------------------------------------|
| `vacuum_buffer_usage_limit`        | `2MB` (16 buffers) | Per-vacuum buffer-access strategy ring.                                         |
| `autovacuum_naptime`               | `1min`           | Sleep between autovacuum sweeps.                                                |
| `autovacuum_vacuum_threshold`      | `50`             | Min number of dead tuples to trigger autovacuum.                                |

## Checkpointing

| GUC                                | Default          | Touched in       | Effect                                                                         |
|------------------------------------|------------------|------------------|--------------------------------------------------------------------------------|
| `checkpoint_timeout`               | `5min`           | [16](16_checkpoints_and_recovery.md) | Time-based checkpoint trigger.                                                |
| `checkpoint_completion_target`     | `0.9`            | [16](16_checkpoints_and_recovery.md) | Target fraction of `checkpoint_timeout` over which to spread the I/O.         |
| `max_wal_size`                     | `1GB`            | [16](16_checkpoints_and_recovery.md) | WAL-size-driven checkpoint trigger.                                            |
| `min_wal_size`                     | `80MB`           | [16](16_checkpoints_and_recovery.md) | Minimum WAL kept (recycled, not deleted).                                      |
| `full_page_writes`                 | `on`             | [13](13_visibility_map.md), [21 §10](21_deep_dives.md) | Emit FPI for first dirty after each checkpoint.                                |
| `synchronous_commit`               | `on`             | [09](09_clog.md), [15](15_persistence_and_wal_records.md) | `off` enables async commit; CLOG group_lsn array becomes important.            |

## Replication

| GUC                                | Default          | Touched in       | Effect                                                                         |
|------------------------------------|------------------|------------------|--------------------------------------------------------------------------------|
| `synchronous_standby_names`        | (empty)          | (sync rep)       | Forces commit to wait for named standbys' replay/flush.                          |
| `synchronous_commit`               | `on`             | [15](15_persistence_and_wal_records.md) | `remote_apply`/`remote_flush` interact with sync replication.                   |
| `wal_sender_timeout`               | `60s`            | walsender        | Drop replication connection if no reply.                                        |
| `max_replication_slots`            | `10`             | [16](16_checkpoints_and_recovery.md) (`CheckPointReplicationSlots`) | -                                                                              |

## Inspection / diagnostic GUCs

| GUC                          | Default          | Effect                                                                         |
|------------------------------|------------------|--------------------------------------------------------------------------------|
| `track_activities`           | `on`             | Populate pg_stat_activity. (Out of scope here.)                                 |
| `track_counts`               | `on`             | Populate pg_stat_*. (Out of scope here.)                                        |
| `log_min_messages`           | `warning`        | -                                                                                |
| `log_lock_waits`             | `off`            | -                                                                                |
| `log_temp_files`             | `-1`             | -                                                                                |
| `compute_query_id`           | `auto`           | -                                                                                |

## See also

- For SLRU buffer sizing semantics: [chapter 8](08_slru_framework.md).
- For freeze threshold mechanics: [chapter 12 § wraparound](12_multixact.md), [21 §15](21_deep_dives.md).
- For checkpoint triggering: [chapter 16](16_checkpoints_and_recovery.md).

---

[Up: index.md](index.md)  |  [Prev: appendix_pgdata_layout.md](appendix_pgdata_layout.md)  |  [Next: metadata_quick_reference.md](metadata_quick_reference.md)
