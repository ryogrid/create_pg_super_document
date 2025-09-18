# pgstat_prep_snapshot

## Location
src/backend/utils/activity/pgstat.c: 957 - 977

## Overview
Prepares the statistics snapshot infrastructure by creating the memory context and hash table needed for snapshot operations if they don't already exist.

## Definition


## Detailed Description
This internal function initializes the snapshot infrastructure required for statistics data collection and storage. It ensures that the necessary memory context and statistics hash table are properly set up before snapshot building operations commence. The function operates conditionally, only performing initialization when specific conditions are met, making it safe to call multiple times.

The function handles forced snapshot clearing, checks fetch consistency settings, and creates the snapshot memory context using a small allocation set strategy optimized for statistics data structures. It establishes the hash table that will store the actual statistics entries during snapshot operations.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_clear_snapshot
  - AllocSetContextCreate
  - pgstat_snapshot_create (macro/function)
  - PGSTAT_FETCH_CONSISTENCY_NONE
  - ALLOCSET_SMALL_SIZES
  - PGSTAT_SNAPSHOT_HASH_SIZE
- Called from (representative examples):
  - pgstat_fetch_entry
  - pgstat_build_snapshot

## Notes and Other Information
- Static function for internal use within the pgstat module
- Implements lazy initialization - only creates structures when needed
- Respects fetch consistency settings to avoid unnecessary work
- Uses TopMemoryContext as parent for the snapshot context to ensure proper lifetime management
- The snapshot context uses ALLOCSET_SMALL_SIZES for memory-efficient allocation of statistics entries
- Safe to call multiple times due to conditional initialization logic