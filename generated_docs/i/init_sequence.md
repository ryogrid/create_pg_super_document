# init_sequence

## Location
[src/backend/commands/sequence.c:1129-1189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L1129-L1189)

## Overview
Initializes and retrieves sequence table entries by relation OID, managing hash table storage and sequence relation access.

## Definition
```c
static void init_sequence(Oid relid, SeqTable *p_elm, Relation *p_rel)
```

## Detailed Description
This function serves as the primary entry point for accessing sequence information in PostgreSQL. It manages a hash table that caches sequence metadata for the lifetime of a backend process, providing efficient access to sequence data without repeatedly parsing system catalogs.

The function first ensures the sequence hash table exists, creating it if necessary. It then searches for an existing entry for the given sequence relation OID, or creates a new entry if none exists. For new entries, it initializes all fields to safe default values.

After obtaining the hash table entry, the function opens and locks the sequence relation using lock_and_open_sequence. It also handles the case where a sequence has been transactionally replaced by checking if the relation's file number has changed since the last access, and if so, discards any cached unissued values while preserving the currval() state.

## Parameters / Member Variables
- `relid`: The relation OID of the sequence to initialize
- `p_elm`: Output parameter that receives the SeqTable entry for the sequence
- `p_rel`: Output parameter that receives the opened Relation structure

## Dependencies
- Functions called/Symbols referenced:
  - SeqTable (sequence table entry structure)
  - [create_seq_hashtable](../c/create_seq_hashtable.md) (creates sequence hash table)
  - [hash_search](../h/hash_search.md) (searches/inserts hash table entries)
  - HASH_ENTER (hash operation flag)
  - InvalidRelFileNumber (invalid file number constant)
  - InvalidLocalTransactionId (invalid transaction ID constant)
  - [lock_and_open_sequence](../l/lock_and_open_sequence.md) (locks and opens sequence relation)
- Called from (representative examples):
  - [ResetSequence](../R/ResetSequence.md)
  - [AlterSequence](../A/AlterSequence.md)
  - [SequenceChangePersistence](../S/SequenceChangePersistence.md)
  - [nextval_internal](../n/nextval_internal.md)
  - [currval_oid](../c/currval_oid.md)
  - [do_setval](../d/do_setval.md)
  - [pg_sequence_last_value](../p/pg_sequence_last_value.md)

## Notes and Other Information
- [Hash](../H/Hash.md) table entries persist for the lifetime of a backend process unless explicitly discarded
- The function handles transactional replacement of sequences by checking relfilenode changes
- Cached values are discarded when sequence replacement is detected, but currval() state is preserved
- This is a static function internal to src/backend/commands/sequence.c
- Memory usage from deleted sequences is minimal and generally not a concern