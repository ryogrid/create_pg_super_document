# SequenceChangePersistence

## Location
src/backend/commands/sequence.c: 541 - 569

## Overview
Handles persistence change operations for PostgreSQL sequences, ensuring that sequence state is properly preserved when altering a sequence's persistence characteristics (logged/unlogged).

## Definition


## Detailed Description
This function is responsible for changing the persistence of a sequence relation while maintaining its state and data integrity. It is called during ALTER TABLE operations that change a sequence's persistence characteristics. The function ensures that sequence increments from concurrent nextval() calls are not lost during the persistence change operation by acquiring proper locks and preserving the sequence data through the relfilenode change.

The function performs a complete sequence state preservation cycle: it reads the current sequence tuple, creates a new relfilenode with the new persistence setting, and fills the new relation with the existing sequence data.

## Parameters / Member Variables
- : The OID of the sequence relation whose persistence is being changed
- : The new persistence characteristic ('p' for permanent/logged, 'u' for unlogged, 't' for temporary)

## Dependencies
- Functions called/Symbols referenced:
  - LockRelationOid (with AccessExclusiveLock)
  - init_sequence
  - RelationNeedsWAL
  - GetTopTransactionId
  - read_seq_tuple
  - RelationSetNewRelfilenumber
  - fill_seq_with_data
  - UnlockReleaseBuffer
  - sequence_close
- Called from (representative examples):
  - ATRewriteTables (in tablecmds.c)

## Notes and Other Information
- Acquires AccessExclusiveLock to prevent concurrent nextval() calls from losing increments during the persistence change
- The WAL logging check and GetTopTransactionId() call ensures proper transaction handling for WAL-logged sequences
- Part of the ALTER TABLE infrastructure for handling owned sequences
- Critical for maintaining sequence consistency across persistence changes