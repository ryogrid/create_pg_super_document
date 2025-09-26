# GetFakeLSNForUnloggedRel

## Location
[src/backend/access/transam/xlog.c:4559-4575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4559-L4575)

## Overview
Generates fake LSN values for unlogged relations that provide an increasing sequence without writing any WAL records.

## Definition
```c
XLogRecPtr GetFakeLSNForUnloggedRel(void)
```

## Detailed Description
GetFakeLSNForUnloggedRel provides a mechanism to generate LSN-like values for unlogged relations without the overhead of writing WAL records. Since unlogged relations do not generate WAL records, they cannot use real LSNs from XLogInsert. However, some operations still need monotonically increasing sequence numbers that behave like LSNs.

The function uses an atomic counter (XLogCtl->unloggedLSN) to generate unique, increasing values. Each call increments the counter and returns the previous value, ensuring that every call produces a larger LSN than previous calls. The counter value is preserved across clean shutdowns but is reset after crashes, which is consistent with the behavior of unlogged relations themselves.

This mechanism is particularly useful for operations that need to track page modifications or maintain ordering without the durability guarantees of real LSNs.

## Parameters / Member Variables
This function takes no parameters and returns an XLogRecPtr (LSN value).

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_add_u64](../p/pg_atomic_fetch_add_u64.md) (atomic increment operation)
  - XLogCtl (global WAL control structure)
- Called from (representative examples):
  - [gistGetFakeLSN](../g/gistGetFakeLSN.md)
  - [WALAvailability](../W/WALAvailability.md) (header declaration)

## Notes and Other Information
- The function uses atomic operations to ensure thread safety in multi-process environments
- The counter is saved and restored across clean shutdowns but does not survive crashes
- This provides LSN-like behavior for unlogged relations which don't generate real WAL records
- The fake LSNs are only meaningful within the context of a single database session/cluster lifecycle
- Commonly used by index access methods for unlogged tables that need sequence numbers for internal operations
- Located in src/backend/access/transam/xlog.c:4559-4575