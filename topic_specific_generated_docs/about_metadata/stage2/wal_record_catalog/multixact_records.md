# WAL Record Catalog: MultiXact (RM_MULTIXACT_ID)

`RM_MULTIXACT_ID = "MultiXact"`, redo: `multixact_redo`
(`src/backend/access/transam/multixact.c`).

## XLOG_MULTIXACT_ZERO_OFF_PAGE  (info 0x00)

- **Header**: `multixact.h:68`.
- **Payload**: `int64 pageno` (8 bytes).
- **Emitter**: `GetNewMultiXactId()` when the new multi falls on a fresh
  offsets page.
- **Redo**: `multixact_redo` calls `SimpleLruZeroPage(MultiXactOffsetCtl,
  pageno)` then `SimpleLruWritePage`.
- **Makes durable**: zeroing of a fresh `pg_multixact/offsets` page.
- **Standby effects**: new page on disk.

## XLOG_MULTIXACT_ZERO_MEM_PAGE  (info 0x10)

- **Header**: `multixact.h:69`.
- **Payload**: `int64 pageno`.
- **Emitter**: `GetNewMultiXactId()` when the new offset falls on a fresh
  members page.
- **Redo**: `SimpleLruZeroPage(MultiXactMemberCtl, pageno)`.
- **Makes durable**: zeroing of a fresh `pg_multixact/members` page.

## XLOG_MULTIXACT_CREATE_ID  (info 0x20)

- **Header**: `multixact.h:70`.
- **Payload**:
  ```c
  typedef struct xl_multixact_create
  {
      MultiXactId        mid;
      MultiXactOffset    moff;
      int32              nmembers;
      MultiXactMember    members[FLEXIBLE_ARRAY_MEMBER];
  } xl_multixact_create;
  ```
- **Emitter**: `MultiXactIdCreateFromMembers` → `RecordNewMultiXact`. The
  WAL record is written *before* the SLRU pages are updated.
- **Redo**: `multixact_redo` → `RecordNewMultiXact(mid, moff, nmembers,
  members)` → updates both the offsets SLRU (with `moff` at slot
  `mid`) and the members SLRU (with the member array starting at offset
  `moff`). Also advances `nextMulti` and `nextMultiOffset` cursors in
  shared memory.
- **Makes durable**: creation of one MultiXactId, including its members.
- **Full-page image**: no (the SLRU pages are written via SimpleLruWritePage
  separately; the WAL record carries enough info to recreate them).

## XLOG_MULTIXACT_TRUNCATE_ID  (info 0x30)

- **Header**: `multixact.h:71`.
- **Payload**:
  ```c
  typedef struct xl_multixact_truncate
  {
      Oid              oldestMultiDB;
      MultiXactId      startTruncOff;
      MultiXactId      endTruncOff;
      MultiXactOffset  startTruncMemb;
      MultiXactOffset  endTruncMemb;
  } xl_multixact_truncate;
  ```
- **Emitter**: `TruncateMultiXact()` from `vac_truncate_clog`.
- **Redo**:
  1. Update `oldestMulti` and `oldestMultiDB`.
  2. `SimpleLruTruncate(MultiXactOffsetCtl, ...)`.
  3. `SimpleLruTruncate(MultiXactMemberCtl, ...)`.
- **Makes durable**: simultaneous truncation of both offsets and members
  SLRUs.
- **Standby effects**: shrinks pg_multixact/offsets and members directories.

## Cross-references

- `component_multixact.md` — full design.
- `slru_users_catalog/multixact_offsets.md`, `multixact_members.md`.
