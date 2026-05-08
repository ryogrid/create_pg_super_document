# Appendix G — On-Disk Recovery Layout

[← Recovery Target Quick Reference](appendix_recovery_target_quick_reference.md) | [index](index.md) | [next: GUC Parameters →](appendix_guc_parameters.md)

---

This appendix maps every recovery-relevant on-disk artifact under
`$PGDATA`. The cluster's *recovery state* lives in a small set of
files; this is the operator's view.

## File map

```
$PGDATA/
├── global/
│   └── pg_control                  Cluster control file (CRC-protected)
│                                    - state (DBState)
│                                    - checkPoint LSN + checkPointCopy
│                                    - minRecoveryPoint + minRecoveryPointTLI
│                                    - backupStartPoint + backupEndPoint + backupEndRequired
│                                    - system_identifier, wal_level, etc.
├── backup_label                    [optional, transient]
│                                    Set by pg_basebackup or pg_backup_start.
│                                    Read by InitWalRecovery → read_backup_label.
│                                    Renamed to backup_label.old after consistency.
├── tablespace_map                  [optional, transient]
│                                    Companion to backup_label; describes
│                                    pg_tblspc symlinks. Renamed to .old after
│                                    apply.
├── recovery.signal                 [optional]
│                                    Operator places this to request archive
│                                    recovery. fsync'd on detect; removed when
│                                    recovery completes.
├── standby.signal                  [optional]
│                                    Operator places this to request standby
│                                    mode (= continuous recovery + streaming).
│                                    Persists across restarts; removed at
│                                    promotion.
├── promote                         [optional, transient]
│                                    Created by pg_promote / pg_ctl promote.
│                                    Polled by CheckForStandbyTrigger.
│                                    Removed by RemovePromoteSignalFiles.
├── postgresql.conf                 Recovery GUCs:
├── postgresql.auto.conf              restore_command, primary_conninfo,
│                                     primary_slot_name, recovery_target_*,
│                                     hot_standby, max_standby_*_delay,
│                                     recovery_min_apply_delay, etc.
├── pg_wal/                         Local WAL segments
│   ├── 00000001000000020000003F   Segment file: <8-hex-TLI><24-hex-LogSeg>
│   ├── 00000002.history           Timeline history files (parent TLI,
│   │                                switchpoint LSN, reason — append-only)
│   ├── RECOVERYXLOG               [transient] File restored by
│   │                                restore_command; renamed to
│   │                                <TLI><segno> by KeepFileRestoredFromArchive.
│   ├── RECOVERYHISTORY            [transient] Same for history files.
│   └── archive_status/            Per-segment .ready/.done markers
│                                    consumed by archiver.
├── pg_xact/                        CLOG SLRU
├── pg_subtrans/                    Subxact-parent SLRU
├── pg_multixact/
│   ├── offsets/                    SLRU
│   └── members/                    SLRU
├── pg_commit_ts/                   Commit-timestamp SLRU (if enabled)
├── pg_twophase/                    One file per prepared xact: pg_twophase/<XID>
│                                    Loaded by RestoreTwoPhaseData early in
│                                    StartupXLOG; updated during redo via
│                                    PrepareRedoAdd / PrepareRedoRemove.
├── pg_logical/                     Logical replication state
│   ├── replorigin_checkpoint       Replorigin progress (replayed by
│   │                                replorigin_redo).
│   ├── snapshots/                  Logical snapshot state
│   └── mappings/                   For logical decoding catalog tracking
├── pg_replslot/                    Replication slot state (one dir per slot)
│                                    Slots mirror across recovery; physical
│                                    slots advance with replay; logical slots
│                                    are invalidated on conflict.
├── pg_filenode.map                 Pre-bootstrap relation mapping (rewritten
│                                    by relmap_redo via XLOG_RELMAP_UPDATE).
├── base/<dboid>/
│   ├── pg_filenode.map             Per-database relmap
│   └── ...                         Relation files
└── pg_tblspc/<ts_oid>              Symlinks to tablespace locations (created/
                                     destroyed by tblspc_redo).
```

## Lifecycle of recovery-state files

### Initial state (just after `pg_basebackup`)

```
pg_control:        state = DB_STARTUP, checkPoint set to backup-time chkpt
backup_label:      present, BACKUP METHOD = streamed, BACKUP FROM = primary
tablespace_map:    present (if -F p)
standby.signal:    present (if -R), or absent
postgresql.auto.conf: primary_conninfo, primary_slot_name (if -R)
pg_wal/:            empty or contains starting segments (depending on -X mode)
```

### During recovery (a record has been replayed)

```
pg_control:        state = DB_IN_ARCHIVE_RECOVERY (or DB_IN_CRASH_RECOVERY),
                    minRecoveryPoint advanced
backup_label:      still present until consistency reached
recovery.signal:   present (if it was originally there)
pg_wal/RECOVERYXLOG: latest restored segment (if archive recovery)
```

### After consistency

```
pg_control:        same, plus minRecoveryPoint == backupEndPoint passed
backup_label:      RENAMED to backup_label.old (so a restart doesn't re-read it)
tablespace_map:    RENAMED to tablespace_map.old
```

The renames happen inside `xlog_redo` when `XLOG_BACKUP_END` is replayed
(via `xlogrecovery_redo`'s special handling).

### After promotion / end of archive recovery

```
pg_control:        state = DB_IN_PRODUCTION
                    checkPoint = LSN of new end-of-recovery checkpoint
                    new TLI
new <newTLI>.history: written by writeTimeLineHistory (and archived)
recovery.signal:   REMOVED
standby.signal:    REMOVED
promote:           REMOVED (if existed)
backup_label.old:  remains for forensics
```

## Pre-12 file layout note

PostgreSQL ≤ 11 used a single file:

```
$PGDATA/recovery.conf    [pre-12 only]
```

This contained all recovery GUCs plus the implicit "I want to recover"
signal. PG ≥ 12:

* GUCs moved to `postgresql.conf` / `postgresql.auto.conf`.
* The "recovery requested" signal moved to `recovery.signal` (PITR)
  and `standby.signal` (replication).

If `recovery.conf` is found in `$PGDATA` on a PG ≥ 12, the server
**refuses to start** (intentionally) with a hint to migrate.

## Diagnostic queries

| Query | Reads from |
|-------|-----------|
| `SELECT pg_is_in_recovery();` | `RecoveryInProgress()` ⇒ `XLogCtl->SharedRecoveryState` |
| `SELECT pg_last_wal_receive_lsn();` | `WalRcv->flushedUpto` |
| `SELECT pg_last_wal_replay_lsn();` | `XLogRecoveryCtl->lastReplayedEndRecPtr` |
| `SELECT pg_last_xact_replay_timestamp();` | `XLogRecoveryCtl->recoveryLastXTime` |
| `SELECT pg_get_wal_replay_pause_state();` | `XLogRecoveryCtl->recoveryPauseState` |
| `SELECT * FROM pg_control_recovery();` | `pg_control` fields |
| `SELECT * FROM pg_stat_recovery_prefetch;` | `XLogPrefetcher` counters |
| `SELECT * FROM pg_stat_replication;` | walsenders (cascade only) |

## See also

* [06_signal_files_and_pg_control.md](06_signal_files_and_pg_control.md)
* [appendix_data_structures.md](appendix_data_structures.md) for `ControlFileData`.
