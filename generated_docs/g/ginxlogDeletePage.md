# ginxlogDeletePage

## Location
[src/include/access/ginxlog.h:155-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginxlog.h#L155-L160)

## Overview
Structure used for WAL (Write-Ahead Logging) record when deleting pages from a GIN index during vacuum operations.

## Definition
```c
typedef struct ginxlogDeletePage
{
    OffsetNumber parentOffset;
    BlockNumber rightLink;
    TransactionId deleteXid;    /* last Xid which could see this page in scan */
} ginxlogDeletePage;
```

## Detailed Description
The ginxlogDeletePage structure is used as part of WAL logging when vacuum operations delete empty or obsolete pages from GIN (Generalized Inverted Index) structures. This operation is part of the vacuum process that reclaims space by removing pages that are no longer needed. The structure stores metadata necessary to properly replay the page deletion during recovery, including the parent page relationship, link structure updates, and transaction visibility information.

## Parameters / Member Variables
- `parentOffset`: Offset number in the parent page that points to the page being deleted
- `rightLink`: Block number of the right sibling page (used to update left sibling's right link)
- `deleteXid`: Transaction ID of the last transaction that could see this page during a scan (used for visibility and MVCC purposes)

## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber
  - BlockNumber 
  - TransactionId
- Called from (representative examples):
  - [ginDeletePage](ginDeletePage.md) (in src/backend/access/gin/ginvacuum.c:201,220)
  - [ginRedoDeletePage](ginRedoDeletePage.md) (in src/backend/access/gin/ginxlog.c:480)

## Notes and Other Information
- This structure is part of the GIN index WAL logging infrastructure (XLOG_GIN_DELETE_PAGE operation)
- Used specifically during vacuum operations to remove empty or obsolete pages
- The deleteXid field is crucial for maintaining MVCC semantics and ensuring visibility correctness
- During recovery, ginRedoDeletePage uses this data to update parent and sibling page links
- The operation involves updating multiple pages: the deleted page, its parent, and its left sibling
- Defined in src/include/access/ginxlog.h:155-160