# SimpleLruWritePage

## Location
[src/backend/access/transam/slru.c:729-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L729-L742)

## Overview
Public wrapper function that provides external callers access to SLRU page writing functionality without exposing internal flush operations.

## Definition

```c
void
SimpleLruWritePage(SlruCtl ctl, int slotno)
```
## Detailed Description
SimpleLruWritePage serves as a public interface to the internal SLRU page writing mechanism. It acts as a thin wrapper around SlruInternalWritePage, specifically designed for external callers who need to write individual pages but don't require the advanced flush operations available through the internal interface.

The function performs basic validation to ensure the slot contains valid data (not empty) before delegating the actual write operation to SlruInternalWritePage with NULL for the flush data parameter. This design pattern provides a clean separation between internal operations (which may need flush capabilities for checkpoints) and external operations (which typically write individual pages as needed).

This function is commonly used during bootstrap operations and WAL replay when specific SLRU pages need to be written to disk to ensure data consistency and durability.

## Parameters / Member Variables
- `ctl`: SlruCtl control structure containing SLRU configuration and shared state
- `slotno`: Integer slot number identifying which buffer slot contains the page to write
## Dependencies
- Functions called/Symbols referenced:
  - [SlruInternalWritePage](SlruInternalWritePage.md)
  - SLRU_PAGE_EMPTY (status constant)
- Called from (representative examples):
  - [BootStrapCLOG](../B/BootStrapCLOG.md)
  - [clog_redo](../c/clog_redo.md)
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [commit_ts_redo](../c/commit_ts_redo.md)
  - [BootStrapMultiXact](../B/BootStrapMultiXact.md)
  - [multixact_redo](../m/multixact_redo.md)
  - [BootStrapSUBTRANS](../B/BootStrapSUBTRANS.md)

## Notes and Other Information
- Always passes NULL as the fdata parameter to SlruInternalWritePage, meaning it never participates in checkpoint flush operations
- Used extensively during database bootstrap and WAL replay operations
- Provides the primary public interface for SLRU page writing in PostgreSQL subsystems like CLOG, commit timestamps, multixact, and subtransaction status
- Requires the appropriate bank lock to be held by the caller (inherited requirement from SlruInternalWritePage)
- Part of PostgreSQL's transaction status management infrastructure

## Simplified Source
```c
// Public wrapper for writing an SLRU page to disk
void SimpleLruWritePage(SlruCtl ctl, int slotno)
{
    // Verify the slot contains valid data
    Assert(ctl->shared->page_status[slotno] != SLRU_PAGE_EMPTY);

    // Write the page using internal function (no flush data)
    SlruInternalWritePage(ctl, slotno, NULL);
}
```