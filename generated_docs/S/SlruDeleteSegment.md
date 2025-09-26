# SlruDeleteSegment

## Location
[src/backend/access/transam/slru.c:1523-1599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1523-L1599)

## Overview
Public function to delete an individual SLRU segment, ensuring all buffer references are cleaned out before physical deletion.

## Definition
```c
void SlruDeleteSegment(SlruCtl ctl, int64 segno)
```

## Detailed Description
SlruDeleteSegment provides a safe way to delete an SLRU segment by first cleaning out any existing references to the segment in the SLRU buffer pool, then calling SlruInternalDeleteSegment to perform the actual file deletion. The function uses a sophisticated locking mechanism with bank locks to ensure thread safety while scanning through all slots.

The function performs these key operations:
1. Acquires appropriate bank locks for thread-safe buffer scanning
2. Scans all SLRU slots to find pages belonging to the target segment
3. For clean pages, marks them as empty; for dirty pages, writes them out first
4. Uses a restart mechanism to handle cases where new pages are loaded during I/O
5. Finally calls SlruInternalDeleteSegment to delete the physical file

The bank locking mechanism minimizes contention by only holding locks on relevant banks, switching locks as needed when scanning across different banks.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and configuration
- `segno`: int64 segment number identifying which SLRU segment to delete

## Dependencies
- Functions called/Symbols referenced:
  - SlotGetBankNumber (to determine which bank lock to use)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for bank locking)
  - [SlruInternalWritePage](SlruInternalWritePage.md) (to flush dirty pages)
  - [SimpleLruWaitIO](SimpleLruWaitIO.md) (to wait for ongoing I/O)
  - [SlruInternalDeleteSegment](SlruInternalDeleteSegment.md) (to perform actual file deletion)
- Called from (representative examples):
  - [PerformMembersTruncation](../P/PerformMembersTruncation.md) (in multixact.c)
  - [test_slru_page_delete](../t/test_slru_page_delete.md) (in test code)

## Notes and Other Information
- This is a public function exposed to other PostgreSQL subsystems
- Uses restart logic to handle cases where I/O operations might load new pages
- Employs bank-based locking to reduce contention in multi-slot scenarios
- Ensures data consistency by writing out dirty pages before deletion
- Part of PostgreSQL's SLRU subsystem used for transaction logs like CLOG, subtrans, etc.
- The function is designed to be safe even when concurrent operations might be accessing the SLRU