# SplitPageLayout

## Location
[src/include/access/gist_private.h:191-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L191-L201)

## Overview
SplitPageLayout is a structure that represents the result of the gistSplit function in PostgreSQL's GiST (Generalized Search Tree) index implementation, containing all necessary information for a page split operation.

## Definition


## Detailed Description
SplitPageLayout serves as a container for all data needed during GiST index page split operations. When a page becomes full and needs to be split, this structure holds the reorganized data, including the new page layout, tuples to be placed, and metadata required for WAL (Write-Ahead Logging) operations. The structure forms a linked list through the 'next' pointer, allowing multiple split pages to be processed together during complex split scenarios.

## Parameters / Member Variables
- : gistxlogPage structure containing WAL logging information for the page
- : Pointer to an array of IndexTupleData representing the tuples to be placed on this page
- : Integer count of tuples in the list array
- : IndexTuple representing the union key (bounding box) for this page
- : Page pointer to the actual page being operated on
- : Buffer reference for writing the page after all operations are complete
- : Pointer to the next SplitPageLayout in a linked list for handling multiple split pages

## Dependencies
- Functions called/Symbols referenced:
  - [gistxlogPage](../g/gistxlogPage.md)
  - [IndexTupleData](../I/IndexTupleData.md)
- Called from (representative examples):
  - [gistplacetopage](../g/gistplacetopage.md)
  - [gistfinishsplit](../g/gistfinishsplit.md)
  - [gistSplit](../g/gistSplit.md)
  - [gist_indexsortbuild_levelstate_flush](../g/gist_indexsortbuild_levelstate_flush.md)
  - [gistXLogSplit](../g/gistXLogSplit.md)

## Notes and Other Information
This structure is critical for maintaining ACID properties during GiST index modifications, as it ensures all necessary information is available for both the immediate operation and potential recovery scenarios. The linked list design allows for efficient handling of cascading splits that may occur when parent pages also need to be split to accommodate new entries.