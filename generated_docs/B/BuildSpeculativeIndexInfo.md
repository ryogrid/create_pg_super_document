# BuildSpeculativeIndexInfo

## Location
[src/backend/catalog/index.c:2642-2701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2642-L2701)

## Overview
BuildSpeculativeIndexInfo augments an IndexInfo structure with additional metadata required for speculative insertion operations on unique indexes.

## Definition

```c
struct values[] and isnull[] arrays for a new index tuple.
 *
 *	indexInfo		Info about the index
 *	slot			Heap tuple for which we must prepare an index entry
 *	estate			executor state for evaluating any index expressions
 *	values			Array of index Datums (output area)
 *	isnull			Array of is-null indicators (output area)
 *
 * When there are no index expressions, estate may be NULL.  Otherwise it
 * must be supplied, *and* the ecxt_scantuple slot of its per-tuple expr
 * context must point to the heap tuple passed in.
 *
 * Notice we don't actually call index_form_tuple() here;
```
## Detailed Description
BuildSpeculativeIndexInfo extends an existing IndexInfo structure with specialized information needed to support speculative insertion in unique B-tree indexes. This function is specifically designed for PostgreSQL's speculative insertion mechanism, which allows for optimistic insertion followed by uniqueness checking. The function allocates and populates arrays for unique operators, procedure OIDs, and strategy numbers that are used during the speculative insertion process. This processing is done separately from BuildIndexInfo() to avoid overhead in common non-speculative cases, ensuring optimal performance for regular index operations.

## Parameters / Member Variables
- : Relation structure representing the index being prepared for speculative insertion
- : IndexInfo structure to be augmented with speculative insertion metadata

## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfo](../I/IndexInfo.md) (structure type)
  - IndexRelationGetNumberOfKeyAttributes (function)
  - [get_opfamily_member](../g/get_opfamily_member.md) (function) 
  - [get_opcode](../g/get_opcode.md) (function)
- Called from (representative examples):
  - [ExecOpenIndices](../E/ExecOpenIndices.md)

## Notes and Other Information
- Only supports B-tree indexes (BTREE_AM_OID) and will error for other access methods
- Requires the index to be unique (asserted with ii->ii_Unique)
- Allocates memory for three arrays: ii_UniqueOps, ii_UniqueProcs, and ii_UniqueStrats
- Uses BTEqualStrategyNumber strategy for all key attributes
- Performs validation to ensure required operators exist in the opfamily
- This function is part of PostgreSQL's speculative insertion optimization that reduces lock contention during concurrent unique constraint checking