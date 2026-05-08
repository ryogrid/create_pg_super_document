# Appendix — $PGDATA Layout

[Up: index.md](index.md)  |  [Prev: appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md)  |  [Next: appendix_guc_parameters.md](appendix_guc_parameters.md)

The on-disk layout of a PostgreSQL data directory, annotated with the
subsystem that owns each path. Many of these directories are
documented in detail in their respective chapters; this appendix is
your one-glance overview.

## Top level

```
$PGDATA/
├── PG_VERSION              one-line file: major version (e.g. "17")
├── postgresql.conf         primary GUC file
├── postgresql.auto.conf    ALTER SYSTEM SET overrides
├── pg_hba.conf             host-based authentication
├── pg_ident.conf           user-name maps
├── postmaster.pid          live-postmaster pidfile
├── postmaster.opts         startup-options snapshot
└── ...
```

## global/ — cluster-wide files

```
global/
├── pg_control                  ★ cluster anchor (chapter 03, 21 §17)
├── pg_filenode.map             ★ relmapper for shared catalogs (chapter 07)
├── pg_internal.init            relcache shortcut (shared) (chapter 05, 21 §2)
├── <relfilenode>               physical files for the 11 shared catalogs
├── <relfilenode>_fsm           FSM forks
├── <relfilenode>_vm            VM forks (none for shared catalogs typically)
└── pgstat.stat                 pg_stat persistence (statistics)
```

★ marks the only files written outside of normal heap WAL: pg_control
is updated by `UpdateControlFile`, pg_filenode.map by `write_relmap_file`.

## base/ — per-database directories

```
base/
└── <dboid>/
    ├── PG_VERSION
    ├── pg_filenode.map         ★ relmapper for nailed local catalogs
    ├── pg_internal.init        relcache shortcut (per-db)
    ├── <relfilenode>           heap fork of every local relation
    ├── <relfilenode>_fsm       FSM fork (chapter 14)
    ├── <relfilenode>_vm        VM fork (chapter 13)
    ├── <relfilenode>_init      INIT fork (unlogged relations only)
    └── ...
```

There is one `base/<dboid>/` per database, including the cluster's
`template0`, `template1`, and `postgres` databases.

## SLRU directories

Each SLRU instance has its own directory under `$PGDATA/`. See
[chapter 19](19_slru_users_catalog.md) and
[appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)
for per-SLRU details.

```
pg_xact/
├── 0000                        first segment (32 pages × 8 KiB)
├── 0001
└── ...

pg_subtrans/
├── 0000
└── ...

pg_multixact/
├── offsets/
│   ├── 0000
│   └── ...
└── members/
    ├── 0000                    long segment names (64-bit offset)
    └── ...

pg_commit_ts/                   only populated when track_commit_timestamp = on
├── 0000
└── ...

pg_serial/                      SSI; volatile across restarts
├── 0000
└── ...

pg_notify/                      LISTEN/NOTIFY queue; volatile
├── 0000
└── ...
```

## WAL and replication

```
pg_wal/                         ★ the WAL stream (formerly pg_xlog)
├── 000000010000000000000001    16 MiB segment (default)
├── 000000010000000000000002
├── archive_status/             status flags for archive_command
└── summaries/                  WAL summary files (since PG 17)

pg_logical/                     logical replication state
├── mappings/
├── snapshots/
└── replorigin_checkpoint

pg_replslot/                    physical + logical replication slots
└── <slotname>/

pg_logical/replorigin_checkpoint  replication origin state
```

## Two-phase commit

```
pg_twophase/
└── <gid>                       persisted prepared-xact state
```

## Tablespaces and other directories

```
pg_tblspc/                      symlinks to tablespace mount points
└── <tsoid> -> /mnt/storage/...

pg_dynshmem/                    dynamic shared-memory state files
└── ...

pg_stat_tmp/                    transient stat collector temp files
└── ...

pg_stat/                        post-shutdown stats persistence
└── ...

pg_snapshots/                   exported snapshot files
└── ...
```

## Subsystem ownership map

| Path                           | Subsystem                                                | Chapter                                            |
|--------------------------------|----------------------------------------------------------|----------------------------------------------------|
| `global/pg_control`            | pg_control / cluster anchor                              | [03](03_catalog_data_model_and_bootstrap.md), [21 §17](21_deep_dives.md) |
| `global/pg_filenode.map`       | relmapper (shared)                                       | [07](07_relmapper.md)                              |
| `base/<dbid>/pg_filenode.map`  | relmapper (local nailed)                                 | [07](07_relmapper.md)                              |
| `global/pg_internal.init`      | relcache shortcut (shared)                                | [05](05_catalog_caches.md), [21 §2](21_deep_dives.md) |
| `base/<dbid>/pg_internal.init` | relcache shortcut (per-db)                                | [05](05_catalog_caches.md)                          |
| `pg_xact/`                     | CLOG                                                      | [09](09_clog.md), [19](19_slru_users_catalog.md)    |
| `pg_subtrans/`                 | SUBTRANS                                                  | [10](10_subtrans.md), [19](19_slru_users_catalog.md)|
| `pg_multixact/offsets/`        | MultiXact offsets                                         | [12](12_multixact.md), [19](19_slru_users_catalog.md)|
| `pg_multixact/members/`        | MultiXact members                                         | [12](12_multixact.md), [19](19_slru_users_catalog.md)|
| `pg_commit_ts/`                | Commit timestamps                                         | [11](11_commit_timestamps.md), [19](19_slru_users_catalog.md)|
| `pg_serial/`                   | SSI predicate-lock SLRU                                   | [19](19_slru_users_catalog.md)                     |
| `pg_notify/`                   | LISTEN/NOTIFY queue                                       | [19](19_slru_users_catalog.md)                     |
| `pg_wal/`                      | WAL stream                                                | [15](15_persistence_and_wal_records.md), [16](16_checkpoints_and_recovery.md) |
| `pg_twophase/`                 | 2PC state                                                 | [16](16_checkpoints_and_recovery.md) (`CheckPointTwoPhase`) |
| `pg_logical/`                  | Logical replication                                       | [18](18_catalog_inventory.md) §replication        |
| `pg_replslot/`                 | Replication slots                                         | [16](16_checkpoints_and_recovery.md) (`CheckPointReplicationSlots`) |
| `pg_tblspc/`                   | Tablespace symlinks                                       | [20](20_wal_record_catalog.md) (XLOG_TBLSPC_*)    |
| `pg_stat_tmp/`, `pg_stat/`     | Statistics collector                                      | (out of scope for this document)                  |
| `pg_dynshmem/`                 | Dynamic shared memory                                     | (out of scope)                                    |
| `pg_snapshots/`                | Exported snapshots                                        | (out of scope)                                    |
| `base/<dbid>/<relnode>_fsm`    | FSM fork                                                  | [14](14_free_space_map.md)                         |
| `base/<dbid>/<relnode>_vm`     | VM fork                                                   | [13](13_visibility_map.md)                         |

---

[Up: index.md](index.md)  |  [Prev: appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md)  |  [Next: appendix_guc_parameters.md](appendix_guc_parameters.md)
