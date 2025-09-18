# test_slru_shmem_startup

## Location
[src/test/modules/test_slru/test_slru.c:214-248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L214-L248)

## Overview
A shared memory startup hook function that initializes the test SLRU (Simple LRU) module by setting up the SLRU directory, lock tranches, and the SLRU control structure during PostgreSQL's shared memory initialization phase.

## Definition


## Detailed Description
This function serves as a shmem_startup_hook callback in the test_slru module, responsible for the complete initialization of the test SLRU system. It first calls any previously registered shared memory startup hook to maintain the hook chain. The function then creates the necessary SLRU directory ("pg_test_slru") if it doesn't exist, sets up LWLock tranches for synchronization, configures the SLRU control structure with the page precedence callback, and finally initializes the SLRU system using SimpleLruInit. The function specifically focuses on testing long segment names as indicated by the long_segment_names parameter set to true.

## Parameters / Member Variables
- No parameters (void function)

## Local Variables
- : Boolean flag set to true to test long segment names functionality
- : String constant "pg_test_slru" - directory name for SLRU files
- : Integer holding the LWLock tranche ID for the main SLRU operations
- : Integer holding the LWLock tranche ID for buffer operations

## Dependencies
- Functions called/Symbols referenced:
  - prev_shmem_startup_hook (function pointer, may be NULL)
  - MakePGDirectory
  - LWLockNewTrancheId
  - LWLockRegisterTranche
  - [test_slru_page_precedes_logically](test_slru_page_precedes_logically.md)
  - [SimpleLruInit](../S/SimpleLruInit.md)
  - TestSlruCtl (macro pointing to &TestSlruCtlData)
  - NUM_TEST_BUFFERS (constant: 16)
  - SYNC_HANDLER_NONE
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_slru.c file
- Part of the hook chain mechanism for shared memory startup in PostgreSQL extensions
- Creates the "pg_test_slru" directory in the PostgreSQL data directory if it doesn't exist
- Sets up two separate LWLock tranches: one for general SLRU operations and one for buffer management
- Note: There appears to be a bug in line 239 where test_tranche_id is used instead of test_buffer_tranche_id for the second LWLockRegisterTranche call
- The function specifically tests long segment names functionality by setting long_segment_names to true
- Essential for proper initialization of the test SLRU module's shared memory structures and file system components