# fill_seq_with_data

## Location
src/backend/commands/sequence.c: 338 - 358

## Overview
fill_seq_with_data initializes a sequence relation with specified tuple data, handling both regular and unlogged sequences by writing to appropriate storage forks.

## Definition


## Detailed Description
fill_seq_with_data is a utility function that populates a sequence relation with initial or updated data. It handles the complexity of unlogged sequences which require data to be written to both the main fork and the init fork.

The function operates in two phases:
1. **Main Fork Population**: Always writes the tuple data to the MAIN_FORKNUM using 
2. **Unlogged Sequence Handling**: For unlogged sequences (RELPERSISTENCE_UNLOGGED):
   - Creates the INIT_FORKNUM using storage manager operations
   - Logs the fork creation for WAL purposes via 
   - Writes the same tuple data to the INIT_FORKNUM
   - Flushes relation buffers to ensure data persistence
   - Closes the storage manager relation

This dual-fork approach for unlogged sequences ensures that after a crash, the sequence can be properly initialized from the init fork since unlogged relations lose their main fork data during recovery.

## Parameters / Member Variables
- : Relation representing the sequence to populate
- : HeapTuple containing the sequence data to write (last_value, log_cnt, is_called)

## Dependencies
- Functions called/Symbols referenced:
  - fill_seq_fork_with_data
  - smgropen
  - smgrcreate
  - log_smgrcreate
  - FlushRelationBuffers
  - smgrclose
- Called from (representative examples):
  - DefineSequence (src/backend/commands/sequence.c:217)
  - ResetSequence (src/backend/commands/sequence.c:322)
  - AlterSequence (src/backend/commands/sequence.c:516)
  - SequenceChangePersistence (src/backend/commands/sequence.c:563)

## Notes and Other Information
- This is a static function used internally within sequence.c
- Essential for proper handling of unlogged sequences which need init fork data for crash recovery
- The function ensures transactional consistency by proper buffer management
- Storage manager operations are carefully logged for WAL replay purposes
- Used during sequence creation, reset, alteration, and persistence changes