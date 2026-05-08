# SLRU Users Catalog: MultiXact Members (pg_multixact/members)

## Identity

- **SlruCtl pointer**: `MultiXactMemberCtl`
- **On-disk directory**: `$PGDATA/pg_multixact/members/`
- **Source**: `src/backend/access/transam/multixact.c`
- **Long segment names**: YES (`long_segment_names = true`) — because the
  members offset can grow well beyond the 32-bit segment-name range that
  short names support.

## Per-page layout

- **Entry size**: variable. Members are stored in groups of 4:
  - 1 byte of "flags" packing 4 × 2-bit `MultiXactStatus` values (8 bits used).
  - 4 × 4 bytes for the four `TransactionId` xids.
  - Total per group: 17 bytes for 4 entries.
- **Entries per page**: roughly `(BLCKSZ - SizeOfPageHeaderData) / 4.25 ≈ 1635`.
  The exact constant `MULTIXACT_MEMBERS_PER_PAGE` is computed from
  `MULTIXACT_MEMBER_SAFE_MULTIPLIER = 5` to leave room for partial groups.

Layout helpers (multixact.c):
```c
#define MXOffsetToFlagsOffset(offset)   /* page-byte offset of the flags byte */
#define MXOffsetToFlagsBitShift(offset) /* shift within the flag byte */
#define MXOffsetToMemberOffset(offset)  /* byte offset of the TransactionId */
#define MXOffsetToMemberPage(offset)    ((offset) / MULTIXACT_MEMBERS_PER_PAGE)
```

## Page-number formula

`MXOffsetToMemberPage(offset) = offset / MULTIXACT_MEMBERS_PER_PAGE`.

`offset` is the absolute member-number, stored in `pg_multixact/offsets`.

## Bank-lock partitioning

Same scheme; default `nslots` from `multixact_member_buffers` GUC.

## Bootstrap path

`SimpleLruInit(MultiXactMemberCtl, "MultiXactMember", nslots, 0,
"pg_multixact/members", ..., SYNC_HANDLER_MULTIXACT_MEMBER, true)`
at `multixact.c:1972`.

## Recovery path

`StartupMultiXact` and `TrimMultiXact` cover both offsets and members.
Members trimming zeroes the trailing portion of the live members page beyond
`nextMultiOffset`.

## Checkpoint hook

`CheckPointMultiXact` (shared with offsets).

## Extend / Truncate

- **Extend**: implicit when `RecordNewMultiXact` writes onto a fresh page;
  emits `XLOG_MULTIXACT_ZERO_MEM_PAGE`.
- **Truncate**: `TruncateMultiXact` truncates members up to the offset
  belonging to `newOldestMulti`. Driven by the same WAL record
  `XLOG_MULTIXACT_TRUNCATE_ID`.

## WAL records

| info | name                          | payload              |
|------|-------------------------------|----------------------|
| 0x10 | XLOG_MULTIXACT_ZERO_MEM_PAGE  | int64 pageno         |
| 0x20 | XLOG_MULTIXACT_CREATE_ID      | xl_multixact_create  |
| 0x30 | XLOG_MULTIXACT_TRUNCATE_ID    | xl_multixact_truncate|

(0x20 and 0x30 are also documented under offsets — they affect both SLRUs.)

## Wraparound considerations

The member offset is also 32-bit and wraps independently of the multi-id
counter. A multi with many members (e.g., a popular row locked by 100
distinct transactions) consumes 100 member offsets. The
`MultiXactMemberFreezeThreshold` function approximates "average members per
multi" and is used to gate vacuum's freezing aggressiveness.

If members run out before multis: `GetNewMultiXactId` ereports an error
referencing `multixact members exhausted`. Operators must run aggressive
vacuum to advance `oldestMulti`.

## Retention

Members older than the offset belonging to `oldestMulti` are truncated.

## Cross-references

- `component_multixact.md` — full deep dive, especially wraparound.
- `slru_users_catalog/multixact_offsets.md` — companion SLRU.
- `wal_record_catalog/multixact_records.md`.
