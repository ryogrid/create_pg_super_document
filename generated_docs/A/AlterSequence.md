# AlterSequence

## Location
[src/backend/commands/sequence.c:437-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L437-L540)

## Overview
AlterSequence modifies the definition of an existing sequence relation, handling parameter changes and optionally rewriting the sequence data when required.

## Definition

```c
structure */
	(void) read_seq_tuple(seqrel, &buf, &datatuple);
```
## Detailed Description
AlterSequence implements the ALTER SEQUENCE SQL command functionality, allowing modification of sequence parameters while maintaining data consistency and transactional safety.

The function performs these key operations:
1. **Access Control**: Uses  with ownership callback to verify permissions and lock the sequence with ShareRowExclusiveLock
2. **Data Retrieval**: 
   - Opens the sequence relation and reads current sequence data
   - Retrieves sequence metadata from pg_sequence system catalog
   - Creates a working copy of the current sequence tuple
3. **Parameter Processing**: Calls  to validate new parameters and determine if sequence rewrite is needed
4. **Conditional Rewrite**: If parameter changes require it (e.g., changing data type):
   - Gets top transaction ID for WAL purposes
   - Creates new storage file using 
   - Writes updated data using 
5. **Cache Management**: Clears local sequence cache while preserving currval() state
6. **Ownership Processing**: Handles OWNED BY clauses if specified
7. **Catalog Updates**: Updates pg_sequence catalog with new metadata
8. **Hook Invocation**: Triggers post-alter hooks for extensibility

The operation is fully transactional - if the transaction aborts, all changes are rolled back.

## Parameters / Member Variables
- : ParseState for query parsing context and error reporting
- : AlterSeqStmt containing the sequence name, options to modify, and control flags (missing_ok, for_identity)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [RangeVarCallbackOwnsRelation](../R/RangeVarCallbackOwnsRelation.md)
  - [init_sequence](../i/init_sequence.md)
  - SearchSysCacheCopy1
  - [read_seq_tuple](../r/read_seq_tuple.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [init_params](../i/init_params.md)
  - RelationNeedsWAL
  - [GetTopTransactionId](../G/GetTopTransactionId.md)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [process_owned_by](../p/process_owned_by.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - [sequence_close](../s/sequence_close.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (src/backend/tcop/utility.c:1671)

## Notes and Other Information
- Supports missing_ok option to avoid errors when sequence doesn't exist
- Uses ShareRowExclusiveLock to allow concurrent reads but prevent conflicting alterations
- Sequence rewrites are only performed when necessary (e.g., type changes, major parameter modifications)
- Cache clearing ensures that cached sequence values don't become stale after parameter changes
- The function preserves currval() state across alterations for user session consistency
- Proper integration with the extension system and object dependency tracking
- Transaction safety ensured through proper buffer management and WAL logging