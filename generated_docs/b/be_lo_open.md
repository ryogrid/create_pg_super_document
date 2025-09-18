# be_lo_open

## Location
src/backend/libpq/be-fsstubs.c: 87 - 125

## Overview
Opens a PostgreSQL large object and returns a file descriptor that can be used for subsequent read/write operations on the large object.

## Definition


## Detailed Description
The  function is a PostgreSQL backend function that opens a large object (LO) for read/write operations. It takes an OID identifying the large object and an access mode, then returns a file descriptor that can be used for subsequent operations. The function handles transaction context management, snapshot registration, and access permission checks. It creates a LargeObjectDesc structure to track the opened large object and associates it with a file descriptor in the global cookies array.

## Parameters / Member Variables
- : Large object OID to open
- : Access mode flags (INV_READ, INV_WRITE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - LargeObjectDesc
  - DEBUG4
  - INV_WRITE
  - PreventCommandIfReadOnly
  - newLOfd
  - inv_open
  - GetCurrentSubTransactionId
  - RegisterSnapshotOnOwner
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL function call mechanism)

## Notes and Other Information
- Performs read-only transaction checks when opened with INV_WRITE mode
- Manages transaction-level resource ownership for snapshots
- Creates filesystem context (fscxt) if this is the first LO opened in the transaction
- Returns a PostgreSQL Datum containing the file descriptor as INT32
- Includes debug logging when FSDB is defined