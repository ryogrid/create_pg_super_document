# spgxlogVacuumRoot

## Location
src/include/access/spgxlog.h: 225 - 234

## Overview
The spgxlogVacuumRoot struct is a PostgreSQL WAL record structure used to log vacuum operations on SP-GiST root pages that are also leaf pages, handling the special case where the index has only one page.

## Definition


## Detailed Description
This structure represents a WAL record for vacuuming SP-GiST root pages that simultaneously serve as leaf pages. This scenario occurs in small indexes where the entire index fits on a single page - the root page contains both the index structure and the actual data tuples. Unlike regular leaf page vacuuming, root page vacuuming has simpler requirements since there are no parent pages to maintain consistency with and no complex chaining operations. The operation primarily involves removing dead tuples from the root/leaf page.

## Parameters / Member Variables
- : Number of tuples that need to be deleted from the root page during the vacuum operation
- : SP-GiST state information containing transaction ID and build flag for the source page
- : Flexible array member containing the offset numbers of tuples to be deleted from the root page

## Dependencies
- Functions called/Symbols referenced:
  - spgxlogState
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - vacuumLeafRoot (src/backend/access/spgist/spgvacuum.c:411)
  - spgRedoVacuumRoot (src/backend/access/spgist/spgxlog.c:838)
  - spg_desc (src/backend/access/rmgrdesc/spgdesc.c:111)
  - SizeOfSpgxlogVacuumRoot (src/include/access/spgxlog.h:236)

## Notes and Other Information
- This is a specialized vacuum operation for the case where the SP-GiST index consists of only a single page that serves as both root and leaf
- Simpler than regular leaf vacuum operations since there are no chain maintenance requirements or parent page updates needed
- The variable data section contains only a simple array of offset numbers for tuples to be deleted
- This operation is essential for maintaining small SP-GiST indexes efficiently without the overhead of the more complex multi-page vacuum operations
- The root/leaf combination typically occurs in newly created indexes or those with very small datasets