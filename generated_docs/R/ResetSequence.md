# ResetSequence

## Location
[src/backend/commands/sequence.c:262-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L262-L337)

## Overview
ResetSequence resets a sequence to its initial value by creating a new storage file and reinitializing the sequence data transactionally.

## Definition


## Detailed Description
ResetSequence implements sequence reset functionality by creating an entirely new relfilenumber for the sequence, similar to rewriting forms of ALTER TABLE. This approach ensures transactional safety - if the current transaction fails, the sequence is restored to its previous state.

The function performs these key operations:
1. **Validation**: Reads the existing sequence to verify it's a valid sequence relation
2. **Metadata Retrieval**: Looks up the sequence's start value from pg_sequence system catalog
3. **Data Preparation**: Creates a copy of the current sequence tuple and modifies it:
   - Sets  to the original start value
   - Sets  to false (sequence hasn't been used)
   - Resets  to 0
4. **Storage Recreation**: Creates new storage file using 
5. **Data Population**: Writes the reset data to the new storage file
6. **Cache Management**: Clears local sequence cache while preserving currval() state

The function requires AccessExclusiveLock and assumes the caller has proper permissions.

## Parameters / Member Variables
- : OID of the sequence relation to reset

## Dependencies
- Functions called/Symbols referenced:
  - [init_sequence](../i/init_sequence.md)
  - [read_seq_tuple](../r/read_seq_tuple.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md)
  - [fill_seq_with_data](../f/fill_seq_with_data.md)
  - [sequence_close](../s/sequence_close.md)
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (src/backend/commands/tablecmds.c:2222)

## Notes and Other Information
- Used primarily during TRUNCATE operations on tables with identity columns or sequences
- Creates entirely new storage file rather than modifying existing data in-place
- Preserves currval() state but clears cached sequence values
- Ensures relfrozenxid and relminmxid are properly set for the new storage
- The transactional nature means the old storage file remains until transaction commit
- Caller must hold AccessExclusiveLock until end of transaction