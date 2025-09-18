# gistnospace

## Location
src/backend/access/gist/gistutil.c: 58 - 77

## Overview
Determines whether there is insufficient space on a GiST page to accommodate a vector of index tuples, accounting for potential tuple deletion and additional free space.

## Definition
```c
bool gistnospace(Page page, IndexTuple *itvec, int len, OffsetNumber todelete, Size freespace)
```

## Detailed Description
This function performs a space availability check for inserting a vector of index tuples into a GiST page. It calculates the total space required for all tuples in the vector (including their ItemIdData overhead) and compares it against the available space on the page. The function can optionally account for space that would be freed by deleting an existing tuple, and can also consider additional free space that might become available. The function returns true if there is NOT enough space (hence "nospace"), making it suitable for conditional checks where space exhaustion needs to be detected.

## Parameters / Member Variables
- `page`: The target page to check for space availability
- `itvec`: Array of IndexTuple pointers that need to be inserted
- `len`: Number of tuples in the itvec array
- `todelete`: Offset number of an existing tuple to be deleted (InvalidOffsetNumber if none)
- `freespace`: Additional free space to consider in the calculation

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md)
  - InvalidOffsetNumber
  - [ItemIdData](../I/ItemIdData.md)
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md)

## Notes and Other Information
The function name follows a negative logic pattern - it returns true when there is NO space available, which makes it convenient for use in conditional statements like "if (gistnospace(...)) then split page". The calculation includes ItemIdData overhead for each tuple, which is essential for accurate space accounting in PostgreSQL's page layout. When todelete is specified, the function accounts for the space that would be reclaimed by removing that tuple, allowing for more precise space calculations during tuple replacement operations.