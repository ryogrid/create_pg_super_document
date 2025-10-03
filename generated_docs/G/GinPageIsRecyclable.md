# GinPageIsRecyclable

## Location
[src/backend/access/gin/ginvacuum.c:802-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginvacuum.c#L802-L822)

## Overview
A utility function that determines whether a GIN index page can be safely recycled by checking if the page deletion transaction is visible to all active transactions.

## Definition
```c
bool GinPageIsRecyclable(Page page)
```

## Detailed Description
This function implements the safety check for recycling deleted GIN index pages. It uses PostgreSQL's transaction visibility mechanisms to ensure that a deleted page can be safely reused without affecting concurrent transactions that might still need to access the page.

The function performs a multi-step check: first verifying if the page is new (uninitialized), then checking if it's marked as deleted, and finally using the global visibility check to determine if the deletion transaction is visible to all currently active transactions. This ensures that no concurrent scan could encounter a recycled page that it expects to find in its original state.

## Parameters / Member Variables
- `page`: Pointer to the page to be checked for recyclability

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](../P/PageIsNew.md)
  - GinPageIsDeleted
  - GinPageGetDeleteXid
  - TransactionIdIsValid
  - [GlobalVisCheckRemovableXid](GlobalVisCheckRemovableXid.md)
- Called from (representative examples):
  - [GinNewBuffer](GinNewBuffer.md)
  - [ginvacuumcleanup](../g/ginvacuumcleanup.md)

## Notes and Other Information
- Returns true if the page can be safely recycled, false otherwise
- New (uninitialized) pages are immediately recyclable
- Non-deleted pages cannot be recycled
- Pages with invalid deletion transaction IDs are considered recyclable
- Uses PostgreSQL's global visibility checker to ensure transaction safety
- Critical for maintaining transaction isolation and preventing data corruption
- Part of the GIN index page lifecycle management system
- Enables efficient reuse of previously allocated but deleted index pages
- Helps prevent unbounded index growth by allowing page reuse

## Simplified Source

```c
bool
GinPageIsRecyclable(Page page)
{
    TransactionId delete_xid;

    // New pages are always recyclable
    if (PageIsNew(page))
        return true;

    // Non-deleted pages cannot be recycled
    if (!GinPageIsDeleted(page))
        return false;

    // Get the transaction ID that deleted this page
    delete_xid = GinPageGetDeleteXid(page);

    // Invalid transaction IDs mean page is recyclable
    if (!TransactionIdIsValid(delete_xid))
        return true;

    // Check if delete transaction is visible to all active transactions
    // If no backend could still see delete_xid as running, page is safe to recycle
    return GlobalVisCheckRemovableXid(NULL, delete_xid);
}
```