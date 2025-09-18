# CreateTupleDescCopyConstr

## Location
[src/backend/access/common/tupdesc.c:173-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L173-L250)

## Overview
Creates a new TupleDesc by deep copying from an existing TupleDesc, including all constraints, defaults, and missing value specifications.

## Definition


## Detailed Description
This function performs a complete deep copy of a tuple descriptor, including all associated constraint information. Unlike a simple copy, it duplicates all constraint structures (default values, check constraints, missing values) and their associated data. The function first creates a template tuple descriptor with the same number of attributes, then flat-copies the attribute array, and finally deep-copies all constraint-related data structures to ensure complete independence between the original and copied descriptors.

## Parameters / Member Variables
- : The source TupleDesc to copy from, including all its constraints and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md)
  - [TupleConstr](../T/TupleConstr.md)
  - [AttrDefault](../A/AttrDefault.md)
  - AttrMissing
  - [ConstrCheck](ConstrCheck.md)
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [initGISTstate](../i/initGISTstate.md)
  - [ATGetQueueEntry](../A/ATGetQueueEntry.md)
  - [init_tuple_slot](../i/init_tuple_slot.md)
  - [CatalogCacheInitializeCache](CatalogCacheInitializeCache.md)
  - [lookup_rowtype_tupdesc_copy](../l/lookup_rowtype_tupdesc_copy.md)

## Notes and Other Information
- Performs deep copying of all constraint structures including default values, check constraints, and missing values
- Uses palloc0 and palloc for memory allocation of constraint structures
- Copies tuple type identification (tdtypeid and tdtypmod) from source
- Handles NULL constraint pointers gracefully
- Ensures complete independence between source and destination tuple descriptors