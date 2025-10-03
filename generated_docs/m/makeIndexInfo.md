# makeIndexInfo

## Location
[src/backend/nodes/makefuncs.c:808-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L808-L863)

## Overview
Creates and initializes an IndexInfo node structure that contains comprehensive metadata about a database index for use during index operations and query processing.

## Definition

```c
IndexInfo *
makeIndexInfo(int numattrs, int numkeyattrs, Oid amoid, List *expressions,
			  List *predicates, bool unique, bool nulls_not_distinct,
			  bool isready, bool concurrent, bool summarizing)
```
## Detailed Description
This function constructs a complete IndexInfo structure that serves as the primary metadata container for database indexes in PostgreSQL. The IndexInfo structure contains all essential information needed for index creation, maintenance, and utilization during query execution. It handles both regular indexes and specialized types like partial indexes (with predicates), expression indexes, and unique indexes.

The function initializes all fields of the IndexInfo structure, including runtime state fields that will be populated during index operations. It also performs validation checks to ensure the parameter combinations are valid (e.g., summarizing indexes cannot have non-key attributes).

## Parameters / Member Variables
- `numattrs`: Total number of attributes (columns) in the index, including both key and non-key attributes
- `numkeyattrs`: Number of key attributes in the index (must be > 0 and ≤ numattrs)
- `amoid`: Object ID of the access method (index type) to be used
- `*expressions`: List of expressions for expression-based index columns (NULL for simple column indexes)
- `*predicates`: List of predicate expressions for partial indexes (NULL for complete indexes)
- `unique`: Boolean flag indicating whether this is a unique index
- `nulls_not_distinct`: Boolean flag for unique indexes - whether NULL values should be considered distinct
- `isready`: Boolean indicating whether the index is ready for inserts
- `concurrent`: Boolean indicating whether this is a concurrent index operation
- `summarizing`: Boolean indicating whether this is a summarizing index (cannot have non-key attributes)
## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a new node of type IndexInfo
  -  - The main index metadata structure type
  -  - Global variable for current memory allocation context
- Called from (representative examples):
  -  - Main index creation command handler
  -  - Builds IndexInfo from catalog data
  -  - Index compatibility validation
  -  - Concurrent index creation

## Notes and Other Information
- Part of the node creation utilities in PostgreSQL's backend
- The function initializes many fields to default values (NULL, false, 0) that will be populated later during index operations
- Includes validation assertions to ensure parameter consistency
- Sets up memory context tracking for proper cleanup
- Essential for all index-related operations in PostgreSQL's execution engine
- The structure supports advanced index features like exclusion constraints, speculative inserts, and parallel index building

## Simplified Source

```c
IndexInfo *
makeIndexInfo(int numattrs, int numkeyattrs, Oid amoid, List *expressions,
              List *predicates, bool unique, bool nulls_not_distinct,
              bool isready, bool concurrent, bool summarizing)
{
    IndexInfo *n = makeNode(IndexInfo);

    // Set basic index properties
    n->ii_NumIndexAttrs = numattrs;
    n->ii_NumIndexKeyAttrs = numkeyattrs;
    Assert(n->ii_NumIndexKeyAttrs != 0);
    Assert(n->ii_NumIndexKeyAttrs <= n->ii_NumIndexAttrs);

    // Set index characteristics
    n->ii_Unique = unique;
    n->ii_NullsNotDistinct = nulls_not_distinct;
    n->ii_ReadyForInserts = isready;
    n->ii_Concurrent = concurrent;
    n->ii_Summarizing = summarizing;

    // Summarizing indexes cannot contain non-key attributes
    Assert(!summarizing || (numkeyattrs == numattrs));

    // Set expressions and predicates
    n->ii_Expressions = expressions;
    n->ii_ExpressionsState = NIL;
    n->ii_Predicate = predicates;
    n->ii_PredicateState = NULL;

    // Initialize arrays to NULL (will be filled later if needed)
    n->ii_ExclusionOps = NULL;
    n->ii_ExclusionProcs = NULL;
    n->ii_ExclusionStrats = NULL;
    n->ii_UniqueOps = NULL;
    n->ii_UniqueProcs = NULL;
    n->ii_UniqueStrats = NULL;

    // Initialize runtime state fields
    n->ii_CheckedUnchanged = false;
    n->ii_IndexUnchanged = false;
    n->ii_BrokenHotChain = false;
    n->ii_ParallelWorkers = 0;

    // Set access method info
    n->ii_Am = amoid;
    n->ii_AmCache = NULL;
    n->ii_Context = CurrentMemoryContext;

    return n;
}
```