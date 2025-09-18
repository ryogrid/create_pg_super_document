# GinState

## Location
[src/include/access/gin_private.h:57-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin_private.h#L57-L89)

## Overview
GinState is a working data structure that describes the index being operated on, containing essential metadata, tuple descriptors, and opclass support functions for GIN index operations.

## Definition


## Detailed Description
GinState serves as the central working structure for GIN index operations, encapsulating all necessary information about the index being processed. It handles both single-column and multi-column indexes, maintaining tuple descriptors and operator class support functions. For multi-column indexes, it manages the complexity of storing different data types by maintaining separate tuple descriptors for each column. The structure also tracks which optional support functions are available for each column.

## Parameters / Member Variables
- : Relation structure representing the GIN index being operated on
- : Boolean flag indicating whether this is a single-column index
- : Original tuple descriptor showing key types for each index column
- : Array of tuple descriptors for actual leaf tuple rowtypes
- : Compare function for each index column
- : Value extraction function for each column
- : Query extraction function for each column
- : Consistency check function for each column
- : Ternary consistency function for each column
- : Optional partial comparison function for each column
- : Array indicating which columns support partial matching
- : Collation information for each column's support functions

## Dependencies
- Functions called/Symbols referenced:
  - INDEX_MAX_KEYS (constant)
- Called from (representative examples):
  - [initGinState](../i/initGinState.md)
  - ginFormTuple
  - [ginReadTuple](../g/ginReadTuple.md)
  - [ginExtractEntries](../g/ginExtractEntries.md)
  - ginInsert operations

## Notes and Other Information
- Located in src/include/access/gin_private.h:57-89
- Contains detailed comments explaining tuple descriptor usage for multi-column indexes
- Leaf tuples contain additional data beyond what TupleDesc knows (see access/gin/README)
- Critical for maintaining operator class function interfaces across different GIN operations
- Supports up to INDEX_MAX_KEYS columns per index