# brin_summarize_range

## Location
src/backend/access/brin/brin.c: 1371 - 1481

## Overview
A SQL-callable function that summarizes a specific page range in a BRIN index, or all unsummarized ranges if the special value BRIN_ALL_BLOCKRANGES is provided.

## Definition


## Detailed Description
This function provides the core functionality for BRIN index maintenance by creating or updating summary tuples for specified block ranges. It performs comprehensive validation including checking that the target is a valid BRIN index, ensuring proper permissions, and verifying that recovery is not in progress. The function implements proper locking protocols (table before index to avoid deadlocks) and security context switching for autovacuum operations. When the special value BRIN_ALL_BLOCKRANGES is passed, it processes all unsummarized ranges in the index.

## Parameters / Member Variables
- : The OID of the BRIN index to be summarized (accessed via )
- : The block number to summarize, or BRIN_ALL_BLOCKRANGES for all blocks (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if database recovery is ongoing
  - : Gets the heap relation OID from index OID
  - /: Opens relations with appropriate locks
  - /: Manages security context for autovacuum
  - /: Security restrictions
  - : Verifies ownership permissions
  - : Performs the actual summarization work
  - : Rolls back GUC changes
  - : Closes relations and releases locks
  - : Special constant for processing all ranges
  - : Lock level used for operations
- Called from (representative examples):
  - : Wrapper function for SQL interface
  - : Autovacuum worker process

## Notes and Other Information
- Blocks operation during recovery with a specific error message about BRIN control functions
- Implements proper deadlock avoidance by locking table before index
- Switches to table owner's userid when called by autovacuum for security
- Validates that the target relation is actually a BRIN index (relam == BRIN_AM_OID)
- Requires table ownership for execution (similar to VACUUM privileges)
- Returns the number of summarized ranges as an integer
- Only processes valid indexes (indisvalid must be true)
- Handles race conditions by rechecking index-to-table mapping after acquiring locks