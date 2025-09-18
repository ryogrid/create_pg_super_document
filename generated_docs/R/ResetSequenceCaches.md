# ResetSequenceCaches

## Location
src/backend/commands/sequence.c: 1887 - 1901

## Overview
ResetSequenceCaches flushes all cached sequence information by destroying the sequence hash table and resetting sequence-related global variables.

## Definition
void ResetSequenceCaches(void)

## Detailed Description
This function serves as a cleanup mechanism for PostgreSQL's sequence caching system. It destroys the global sequence hash table (seqhashtab) if it exists and resets the last_used_seq pointer to NULL. This function is typically called during transaction cleanup or when discarding cached state to ensure that sequence operations start fresh without any cached information that might be stale or invalid.

The function performs two main cleanup operations:
1. Destroys the sequence hash table using hash_destroy() if the table exists
2. Resets the last used sequence pointer to NULL

This ensures that subsequent sequence operations will rebuild the cache from scratch rather than relying on potentially outdated cached data.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - hash_destroy (from hash table utility functions)
- Called from (representative examples):
  - DiscardCommand (in src/backend/commands/discard.c:44)
  - DiscardAll (in src/backend/commands/discard.c:77)

## Notes and Other Information
- This function is part of PostgreSQL's sequence management subsystem
- It operates on global variables seqhashtab and last_used_seq which maintain sequence cache state
- Typically called during DISCARD operations to clean up session state
- Safe to call multiple times - checks if seqhashtab exists before attempting to destroy it
- Essential for preventing memory leaks and ensuring clean session state transitions