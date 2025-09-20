# IndexBuildResult

## Location
[src/include/access/genam.h:30-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/genam.h#L30-L34)

## Overview
IndexBuildResult is a structure that holds statistics returned by the ambuild function during index construction, providing essential metrics about the indexing process.

## Definition

```c
typedef struct IndexBuildResult
{
	double		heap_tuples;	/* # of tuples seen in parent table */
	double		index_tuples;	/* # of tuples inserted into index */
} IndexBuildResult;
```
## Detailed Description
IndexBuildResult serves as a return structure for index access method build functions (ambuild). It encapsulates key statistics generated during the index building process, allowing the system to track how many tuples were processed from the source table versus how many were actually inserted into the index. This information is crucial for understanding index build efficiency and for maintenance operations that depend on accurate tuple counts.

The structure uses double precision floating-point numbers to accommodate potentially very large tuple counts that might exceed the range of integer types in large databases.

## Parameters / Member Variables
- : The total number of tuples that were examined in the parent/source table during the index build process
- : The actual number of tuples that were successfully inserted into the index structure

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md) (BRIN index build)
  - [ginbuild](../g/ginbuild.md) (GIN index build)  
  - [gistbuild](../g/gistbuild.md) (GiST index build)
  - [hashbuild](../h/hashbuild.md) (Hash index build)
  - [btbuild](../b/btbuild.md) (B-tree index build)
  - [spgbuild](../s/spgbuild.md) (SP-GiST index build)
  - [index_build](../i/index_build.md) (generic index building function)

## Notes and Other Information
- The difference between heap_tuples and index_tuples can indicate how many tuples were filtered out during index creation (e.g., due to NULL values in non-partial indexes)
- This structure is defined in src/include/access/genam.h, making it available to all access method implementations
- The use of double precision allows handling extremely large databases without integer overflow concerns
- Various index access methods (B-tree, Hash, GiST, GIN, BRIN, SP-GiST) all use this common structure for reporting build statistics