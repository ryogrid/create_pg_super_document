# IndexOrderByDistance

## Location
[src/include/access/genam.h:125-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/genam.h#L125-L129)

## Overview
IndexOrderByDistance is a structure that represents a nullable distance value used in "ORDER BY column operator constant" clauses for index scans with distance-based ordering.

## Definition
```c
typedef struct IndexOrderByDistance
{
    double      value;
    bool        isnull;
} IndexOrderByDistance;
```

## Detailed Description
IndexOrderByDistance is used to store distance values computed during index scans that support distance-based ordering, particularly in GiST (Generalized Search Tree) and SP-GiST (Space-Partitioned Generalized Search Tree) indexes. This structure enables PostgreSQL to perform nearest-neighbor searches and other distance-based queries efficiently. The structure handles nullable distance values, which is important when distance calculations may not be meaningful or applicable for certain index entries.

## Parameters / Member Variables
- `value`: The computed distance as a double-precision floating-point number
- `isnull`: Boolean flag indicating whether the distance value is NULL (not applicable or undefined)

## Dependencies
- Functions called/Symbols referenced:
  - double (built-in type)
  - [bool](../b/bool.md) (built-in type)

- Called from (representative examples):
  - [gistindex_keytest](../g/gistindex_keytest.md)
  - [gistScanPage](../g/gistScanPage.md)  
  - [index_store_float8_orderby_distances](../i/index_store_float8_orderby_distances.md)
  - [storeGettuple](../s/storeGettuple.md)
  - [GISTSearchItem](../G/GISTSearchItem.md) (as member)
  - [GISTScanOpaqueData](../G/GISTScanOpaqueData.md) (as member)
  - [SpGistScanOpaqueData](../S/SpGistScanOpaqueData.md) (as member)

## Notes and Other Information
- Primarily used in geometric and spatial index operations where distance calculations are fundamental
- The nullable design allows the system to handle cases where distance cannot be computed or is not meaningful
- Essential for implementing nearest-neighbor queries and other proximity-based search operations
- Used internally by GiST and SP-GiST access methods to maintain distance information during index traversal
- The double-precision value provides sufficient accuracy for most distance calculations in spatial applications