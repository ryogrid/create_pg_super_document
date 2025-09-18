# AlterSequence

## Location
src/backend/commands/sequence.c: 437 - 540

## Overview
AlterSequence modifies the definition of an existing sequence relation, handling parameter changes and optionally rewriting the sequence data when required.

## Definition


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
  - RangeVarGetRelidExtended
  - RangeVarCallbackOwnsRelation
  - init_sequence
  - SearchSysCacheCopy1
  - read_seq_tuple
  - heap_copytuple
  - init_params
  - RelationNeedsWAL
  - GetTopTransactionId
  - RelationSetNewRelfilenumber
  - fill_seq_with_data
  - process_owned_by
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - sequence_close
- Called from (representative examples):
  - ProcessUtilitySlow (src/backend/tcop/utility.c:1671)

## Notes and Other Information
- Supports missing_ok option to avoid errors when sequence doesn't exist
- Uses ShareRowExclusiveLock to allow concurrent reads but prevent conflicting alterations
- Sequence rewrites are only performed when necessary (e.g., type changes, major parameter modifications)
- Cache clearing ensures that cached sequence values don't become stale after parameter changes
- The function preserves currval() state across alterations for user session consistency
- Proper integration with the extension system and object dependency tracking
- Transaction safety ensured through proper buffer management and WAL logging