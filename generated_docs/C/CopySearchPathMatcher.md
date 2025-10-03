# CopySearchPathMatcher

## Location
[src/backend/catalog/namespace.c:3889-3910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3889-L3910)

## Overview
Creates a deep copy of an existing SearchPathMatcher structure, allocating the result in the current memory context.

## Definition

```c
SearchPathMatcher *
CopySearchPathMatcher(SearchPathMatcher *path)
```
## Detailed Description
This function performs a deep copy of a SearchPathMatcher structure, creating a new instance with identical configuration but independent memory allocation. It copies all fields including the schema list, catalog/temp namespace flags, and generation number. The function is essential for maintaining separate copies of search path configurations that can be modified independently without affecting the original.

## Parameters / Member Variables
- `*path`: The SearchPathMatcher structure to copy
## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathMatcher](../S/SearchPathMatcher.md) (type)
  - [list_copy](../l/list_copy.md)
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [CopyCachedPlan](CopyCachedPlan.md) (src/backend/utils/cache/plancache.c:1593)
  - RangeVarGetRelid (src/include/catalog/namespace.h:169)

## Notes and Other Information
- Allocates the result in CurrentMemoryContext rather than allowing caller to specify context
- Performs a deep copy of the schema list to ensure complete independence from the original
- Used primarily in plan caching scenarios where search path configurations need to be preserved
- Part of PostgreSQL's namespace resolution and query plan caching infrastructure
- Essential for maintaining search path consistency across different execution contexts