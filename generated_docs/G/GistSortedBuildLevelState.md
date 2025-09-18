# GistSortedBuildLevelState

## Location
[src/backend/access/gist/gistbuild.c:123-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistbuild.c#L123-L129)

## Overview
GistSortedBuildLevelState is a structure used in PostgreSQL's GiST (Generalized Search Tree) index sorted build process to maintain an in-memory buffer of pages at each level of the index hierarchy.

## Definition


## Detailed Description
This structure is a core component of the GiST sorted build algorithm, designed to handle the complexities of building multidimensional indexes efficiently. The sorted build approach uses a stack of these structures, one for each level of the index tree, to maintain an in-memory buffer of the most recent pages at that level.

The structure addresses challenges in sorting multidimensional data where good linearization of the sort opclass is not always guaranteed. To handle these anomalies, the algorithm buffers index tuples and applies multidimension-aware picksplit operations. This approach ensures better spatial locality and improved index quality for multidimensional data types.

Each level state maintains a fixed-size array of pages (defined by GIST_SORTED_BUILD_PAGE_NUM = 4) that serves as a buffer before flushing completed pages to disk. The hierarchical nature is maintained through parent pointers, allowing the build process to work its way up the tree levels as needed.

## Parameters / Member Variables
- : Index of the currently active page within the pages array (0 to GIST_SORTED_BUILD_PAGE_NUM-1)
- : Block number of the last page that was flushed from this level to disk
- : Pointer to the parent level state in the hierarchy; NULL for the root level
- : Fixed-size array of 4 pages that serves as an in-memory buffer for this level

## Dependencies
- Functions called/Symbols referenced:
  - GIST_SORTED_BUILD_PAGE_NUM (constant defining buffer size)
  - Page (PostgreSQL page type)
  - BlockNumber (PostgreSQL block number type)

- Called from (representative examples):
  - [gist_indexsortbuild](../g/gist_indexsortbuild.md) (main sorted build function)
  - [gist_indexsortbuild_levelstate_add](../g/gist_indexsortbuild_levelstate_add.md) (adds tuples to level state)
  - [gist_indexsortbuild_levelstate_flush](../g/gist_indexsortbuild_levelstate_flush.md) (flushes buffered pages)

## Notes and Other Information
- The structure is specifically designed for the sorted build method of GiST indexes, which is an alternative to the traditional build method
- The buffer size of 4 pages (GIST_SORTED_BUILD_PAGE_NUM) represents a balance between memory usage and I/O efficiency
- The hierarchical design with parent pointers enables the build algorithm to propagate changes up the tree as pages are completed
- This approach is particularly beneficial for spatial and multidimensional data where traditional sorting may not preserve spatial locality effectively
- The structure is defined in src/backend/access/gist/gistbuild.c at lines 123-129