# SLRU Users Catalog: MultiXact Offsets (pg_multixact/offsets)

## Identity

- **SlruCtl pointer**: `MultiXactOffsetCtl`
- **On-disk directory**: `$PGDATA/pg_multixact/offsets/`
- **Source**: `src/backend/access/transam/multixact.c`

## Per-page layout

- **Entry size**: 4 bytes per MultiXactId (`MultiXactOffset` = uint32).
- **Entries per page**: `MULTIXACT_OFFSETS_PER_PAGE =
  BLCKSZ / sizeof(MultiXactOffset) = 2048`.

Each entry is the absolute offset (in members units) into the
`pg_multixact/members` SLRU where this multi's first member lives.

## Page-number formula

```c
MultiXactIdToOffsetPage(multi)  = multi / MULTIXACT_OFFSETS_PER_PAGE
MultiXactIdToOffsetEntry(multi) = multi % MULTIXACT_OFFSETS_PER_PAGE
```

## Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`multixact_offset_buffers` GUC.

## Bootstrap path

- `BootStrapMultiXact()`: zero page 0 of both offsets and members SLRUs.
- `MultiXactShmemInit()`: `SimpleLruInit(MultiXactOffsetCtl, "MultiXactOffset",
  nslots, 0, "pg_multixact/offsets", ..., SYNC_HANDLER_MULTIXACT_OFFSET,
  false)` at `multixact.c:1965`.

## Recovery path

- `StartupMultiXact()`:
  - read `nextMulti`, `nextMultiOffset` from `ControlFile->checkPointCopy`.
  - `MultiXactSetNextMXact(nextMulti, nextMultiOffset)`.
  - set `latest_page_number = MultiXactIdToOffsetPage(nextMulti - 1)`.
- `TrimMultiXact()`: zero the trailing portion of the live offsets page.

## Checkpoint hook

```c
void CheckPointMultiXact(void)
{
    SimpleLruWriteAll(MultiXactOffsetCtl, true);
    SimpleLruWriteAll(MultiXactMemberCtl, true);
    /* ... update ControlFile cursors ... */
}
```

## Extend / Truncate

- **Extend**: implicit when `GetNewMultiXactId` advances onto a fresh page;
  emits `XLOG_MULTIXACT_ZERO_OFF_PAGE` and `SimpleLruZeroPage`.
- **Truncate**: `TruncateMultiXact(newOldestMulti, newOldestMultiDB)` from
  `vac_truncate_clog`. Emits `XLOG_MULTIXACT_TRUNCATE_ID` (drives both
  offsets and members truncation).

## WAL records

| info | name                          | payload              |
|------|-------------------------------|----------------------|
| 0x00 | XLOG_MULTIXACT_ZERO_OFF_PAGE  | int64 pageno         |
| 0x20 | XLOG_MULTIXACT_CREATE_ID      | xl_multixact_create  |
| 0x30 | XLOG_MULTIXACT_TRUNCATE_ID    | xl_multixact_truncate|

`multixact_redo` dispatches.

## Wraparound considerations

MultiXactId is 32-bit and wraps at `2^32`. The thresholds:
- `multiVacLimit`: trigger emergency vacuum.
- `multiWarnLimit`: log warning.
- `multiStopLimit`: refuse new multi allocation.

These are computed in `SetMultiXactIdLimit` from `pg_control.oldestMulti`.
Vacuum advances `oldestMulti` by computing `min(datminmxid)` across
pg_database.

## Retention

Pages with all multis older than `oldestMulti` are truncated.

## Cross-references

- `component_multixact.md` — full deep dive.
- `slru_users_catalog/multixact_members.md` — companion SLRU.
- `wal_record_catalog/multixact_records.md`.
