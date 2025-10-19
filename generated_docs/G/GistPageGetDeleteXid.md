# GistPageGetDeleteXid

## Location
[src/include/access/gist.h:215-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist.h#L215-L236)

## Overview
Retrieves the transaction ID of the transaction that deleted a GiST index page, with backward compatibility for older page formats.

## Definition
```c
static inline FullTransactionId
GistPageGetDeleteXid(Page page)
```

## Detailed Description
This function extracts the deletion transaction ID from a GiST index page that has been marked as deleted. It includes backward compatibility logic to handle pages that were deleted before the deleteXid field was introduced. The function first verifies that the page is indeed marked as deleted, then checks if the page contains the deleteXid field by examining the pd_lower value. If the field is present, it returns the stored transaction ID; otherwise, it returns a default value for compatibility with older formats.

The function uses the pd_lower field to determine the page format version - newer pages have enough space allocated to include the deleteXid field, while older pages do not.

## Parameters / Member Variables
- `page`: The deleted GiST index page from which to retrieve the deletion transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - GistPageIsDeleted
  - [PageGetContents](../P/PageGetContents.md)
  - [FullTransactionIdFromEpochAndXid](../F/FullTransactionIdFromEpochAndXid.md)
  - MAXALIGN
  - SizeOfPageHeaderData
  - [GISTDeletedPageContents](GISTDeletedPageContents.md)
  - PageHeader
  - FirstNormalTransactionId
- Called from (representative examples):
  - [gistNewBuffer](../g/gistNewBuffer.md)
  - [gistPageRecyclable](../g/gistPageRecyclable.md)

## Notes and Other Information
- The function includes an assertion that the page must be marked as deleted
- Provides backward compatibility for pages deleted before the deleteXid field was added
- Uses pd_lower to detect the page format version and presence of the deleteXid field
- Returns FirstNormalTransactionId for pages in the old format without the deleteXid field
- This is crucial for the page recycling mechanism to determine when deleted pages can be safely reused
- The function calculates the minimum required pd_lower value to ensure the deleteXid field is present

## Simplified Source

```c
static inline FullTransactionId GistPageGetDeleteXid(Page page) {
    // Ensure page is marked as deleted
    Assert(GistPageIsDeleted(page));

    // Check if deleteXid field is present in this page format
    // (newer pages have sufficient space allocated for the field)
    if (((PageHeader) page)->pd_lower >=
        MAXALIGN(SizeOfPageHeaderData) +
        offsetof(GISTDeletedPageContents, deleteXid) +
        sizeof(FullTransactionId)) {

        // Return the stored deletion transaction ID
        return ((GISTDeletedPageContents *) PageGetContents(page))->deleteXid;
    } else {
        // Backward compatibility: return default for older page format
        return FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId);
    }
}
```