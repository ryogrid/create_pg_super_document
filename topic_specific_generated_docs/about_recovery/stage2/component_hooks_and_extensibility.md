# Hooks and Extensibility

PostgreSQL's recovery subsystem exposes a small number of
extension points. They are intentionally narrow — recovery
correctness depends on tight invariants — but they are sufficient
to implement custom WAL records, custom WAL archive backends, and
out-of-tree replication transports.

[Top index for symbol-by-symbol pages](../../README.md)

## Custom resource managers

### `RegisterCustomRmgr`

```c
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);
```

* Called from an extension's `_PG_init`.
* `rmid` must be in `[RM_MIN_CUSTOM_ID, RM_MAX_CUSTOM_ID]` =
  `[128, 255]`.
* The provided `rm_redo`, `rm_desc`, `rm_identify` are slotted into
  `RmgrTable` and consulted by `ApplyWalRecord` whenever a record
  with that rmid appears.

Used by extensions like Neon (which logs custom records for
storage offload) and Citus (sharding).

### `rm_startup` / `rm_cleanup` extension hooks

Built-in users:

| rmgr | rm_startup | rm_cleanup |
|------|-----------|-----------|
| btree | `btree_xlog_startup` | `btree_xlog_cleanup` |
| gin | `gin_xlog_startup` | `gin_xlog_cleanup` |
| gist | `gist_xlog_startup` | `gist_xlog_cleanup` |
| spgist | `spg_xlog_startup` | `spg_xlog_cleanup` |

These hooks initialize/release per-rmgr state used during the redo
loop (e.g., the incomplete-split tracker hash for btree). Custom
rmgrs may use the same pattern.

### `rm_decode`

For logical decoding integration. Called from
`logical/decode.c` to translate WAL records into logical changes
visible to logical-replication subscribers. Built-in users:
`xact_decode`, `heap_decode`, `heap2_decode`, `standby_decode`,
`logicalmsg_decode`.

A custom rmgr that wants its records to be consumable by logical
replication must implement `rm_decode`.

## `rmgrdesc` plugins for `pg_waldump`

`pg_waldump` calls `rm_desc` to format each record. For a custom
rmgr's records to be human-readable, the operator must point
pg_waldump at a shared library implementing `rmgrdesc`-style
description. The pg_waldump CLI doesn't auto-load — operators
typically set up a script that runs the right plugin.

## `recovery_target_*` GUC hooks

The check_/assign_ hooks are static functions in `xlogrecovery.c`,
not extension points. But the GUCs themselves are extensible
indirectly: an extension could expose its own GUC that, in its
assign hook, sets one of the `recovery_target_*` globals — though
this would require very careful handling because those globals are
`PGC_POSTMASTER`-only.

## Archive command extensibility

`restore_command`, `archive_command`, and `archive_cleanup_command`
are shell strings. They are not C-level hooks, but they are the
*de-facto* extension point for archive backend support: anyone can
write a wrapper script that uses S3, GCS, Azure Blob, etc.

Tools that implement this:

* `pg_archivecleanup` (in-tree, `contrib/pg_archivecleanup`).
* `wal-g`, `pgbackrest`, `barman`, `pgbackman`, etc.

## libpqwalreceiver

The WAL receiver's libpq integration is a *dynamically loaded*
shared library:

```c
load_file("libpqwalreceiver", false);
```

`WalReceiverFunctionsType` is the function pointer table the
loaded library populates. In principle, a custom replication
transport could provide its own `walreceiver` shared library with
a different protocol (e.g., a file-based or RDMA-based replication
transport). In practice this is rarely done outside of
specialized environments.

## Source references

* `src/backend/access/transam/rmgr.c` — `RegisterCustomRmgr`
* `src/include/access/rmgr.h` — `RM_MIN_CUSTOM_ID`,
  `RM_MAX_CUSTOM_ID`
* `src/backend/replication/walreceiver.c` — libpqwalreceiver
  loader
* `src/include/replication/walreceiver.h` —
  `WalReceiverFunctionsType`
