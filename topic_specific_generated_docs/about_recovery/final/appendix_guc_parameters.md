# Appendix H — GUC Parameters

[← PGDATA Recovery Layout](appendix_pgdata_recovery_layout.md) | [index](index.md)

---

Every recovery-relevant GUC. For per-`recovery_target_*` detail, see
[19_recovery_target_catalog.md](19_recovery_target_catalog.md). For
the source code that implements each, see the file:line citations.

## Hot-standby behavior

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `hot_standby` | bool | on | postmaster | If false, standby never opens for queries. PAUSE-target action is demoted to SHUTDOWN if off. |
| `max_standby_archive_delay` | int (ms) | 30000 | sighup | Max wait for backends before canceling on conflict (archive replay). `-1` = wait forever. |
| `max_standby_streaming_delay` | int (ms) | 30000 | sighup | Same, for streaming replay. |
| `recovery_min_apply_delay` | int (ms) | 0 | sighup | Wait *before* applying COMMIT records (commit-time-based). |
| `hot_standby_feedback` | bool | off | sighup | Walreceiver sends current xmin to primary, deferring vacuum. |
| `wal_receiver_status_interval` | int (sec) | 10 | sighup | How often walreceiver sends keepalive/feedback. |
| `wal_receiver_timeout` | int (ms) | 60000 | sighup | Disconnect if no message for this long. |
| `log_recovery_conflict_waits` | bool | off | sighup | Log waits longer than `deadlock_timeout`. |

## Streaming replication client

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `primary_conninfo` | string | "" | sighup | libpq conninfo string used by walreceiver. |
| `primary_slot_name` | string | "" | sighup | Replication slot name on primary. |
| `wal_receiver_create_temp_slot` | bool | off | sighup | Create a temporary slot for this stream if `primary_slot_name` is empty. |

## Archive

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `restore_command` | string | "" | postmaster | Shell command to fetch a WAL segment from the archive. `%f`/`%p`/`%r` substitutions. |
| `archive_cleanup_command` | string | "" | sighup | Shell command run from `CreateRestartPoint` after pg_wal recycling. `%r` = last restartpoint segment. |
| `recovery_end_command` | string | "" | postmaster | Shell command run **once** after `XLOG_END_OF_RECOVERY` is written. |

## Recovery target

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `recovery_target` | string | "" | postmaster | Only `""` or `"immediate"`. |
| `recovery_target_xid` | string→TransactionId | "" | postmaster | Stop at COMMIT/ABORT for this xid. |
| `recovery_target_time` | string→TimestampTz | "" | postmaster | Stop at first COMMIT past this timestamp. |
| `recovery_target_lsn` | string→XLogRecPtr | "" | postmaster | Stop at first record past this LSN. |
| `recovery_target_name` | string | "" | postmaster | Stop at matching `XLOG_RESTORE_POINT` record. |
| `recovery_target_timeline` | string | "latest" | postmaster | TLI to follow. `"latest"` / `"current"` / numeric. |
| `recovery_target_inclusive` | bool | true | postmaster | Whether target record is replayed before stopping. |
| `recovery_target_action` | enum | pause | postmaster | What to do after target is hit. `pause` / `promote` / `shutdown`. |

## Recovery prefetch

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `recovery_prefetch` | enum | try | sighup | `off` / `on` / `try`. Whether to prefetch buffer pages ahead of the redo loop. |
| `maintenance_io_concurrency` | int | 10 | user | Caps in-flight prefetch I/Os for both vacuum and recovery prefetch. |

## Restartpoint and WAL retention

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `checkpoint_timeout` | int (sec) | 300 | sighup | Primary's checkpoint cadence becomes the standby's restartpoint cadence (since standby replays the primary's checkpoint records). |
| `max_wal_size` | int (MB) | 1024 | sighup | Triggers forced restartpoint when approached. |
| `min_wal_size` | int (MB) | 80 | sighup | Floor for recycled segments. |
| `checkpoint_warning` | int (sec) | 30 | sighup | Warn if checkpoint records arrive faster than this. |

## Diagnostic / forensic

| GUC | Type | Default | Context | Effect |
|-----|------|---------|---------|--------|
| `wal_consistency_checking` | string | "" | sighup | Comma-separated rmgr names for which to verify FPI consistency. Slow; for testing only. |

## Cross-reference

* For `recovery_target_*` detail: [19_recovery_target_catalog.md](19_recovery_target_catalog.md).
* For `max_standby_*_delay` semantics: [§10 of Deep Dives](20_deep_dives.md#10-max_standby_streaming_delay-vs-max_standby_archive_delay).
* For `recovery_prefetch` machinery: [04_xlog_reader_and_prefetch.md](04_xlog_reader_and_prefetch.md) and [§4 of Deep Dives](20_deep_dives.md#4-recovery-prefetch-effectiveness).
* For `restore_command` substitution table: [05_archive_fetch_and_restore_command.md](05_archive_fetch_and_restore_command.md).
* For `recovery_min_apply_delay` semantics: [§17 of Deep Dives](20_deep_dives.md#17-recovery_min_apply_delay-semantics).
