# dibuild

## Location
[src/test/modules/dummy_index_am/dummy_index_am.c:139-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/dummy_index_am/dummy_index_am.c#L139-L156)

## Overview
A dummy index build function that simulates building a new index without actually performing any index construction.

## Definition
```c
static IndexBuildResult *dibuild(Relation heap, Relation index, IndexInfo *indexInfo)
```

## Detailed Description
This function is a minimal implementation of an index build routine for PostgreSQL's dummy index access method. It creates and returns an IndexBuildResult structure but does not perform any actual index building operations. The function sets both heap_tuples and index_tuples to 0, indicating that no tuples were processed during the "build" operation. This is appropriate for a dummy access method that serves as a testing framework rather than a functional index implementation.

## Parameters / Member Variables
- `heap`: Relation representing the heap table being indexed
- `index`: Relation representing the index being built  
- `indexInfo`: IndexInfo structure containing metadata about the index being built

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [IndexBuildResult](../I/IndexBuildResult.md) (result structure type)
  - [IndexInfo](../I/IndexInfo.md) (index information structure type)
- Called from (representative examples):
  - [dihandler](dihandler.md) (dummy index access method handler at src/test/modules/dummy_index_am/dummy_index_am.c:304)

## Notes and Other Information
- This function is part of PostgreSQL's test infrastructure for the dummy index access method
- The function is declared as static, limiting its scope to the compilation unit
- Returns a properly allocated IndexBuildResult but with zero tuple counts, reflecting the dummy nature of this access method
- Does not perform any actual indexing work, making it safe for testing purposes
- Located in src/test/modules/dummy_index_am/dummy_index_am.c:139-156
- Serves as a template for implementing actual index build functions in custom access methods

## Simplified Source

```c
static IndexBuildResult *
dibuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;

    // Allocate result structure
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

    // Set dummy values - no actual indexing performed
    result->heap_tuples = 0;     // Pretend no tuples were scanned
    result->index_tuples = 0;    // No index tuples created

    return result;
}
```