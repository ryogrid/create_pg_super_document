# pgstat_detach_shmem

## Location
src/backend/utils/activity/pgstat_shmem.c: 238 - 266

## Overview
Function that cleanly detaches a backend process from the shared memory statistics system during shutdown, releasing all references and resources.

## Definition


## Detailed Description
This function performs cleanup when a backend process is shutting down, detaching from the statistics shared memory structures. It releases all entry references in the shared hash table, detaches from both the hash table and DSA, and manually releases the DSA reference count. The function ensures no dangling references to shared statistics remain when the backend terminates.

## Parameters / Member Variables
- No parameters (void function)
- No return value

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_release_all_entry_refs: Releases all references to statistics entries
  - dshash_detach: Detaches from shared hash table
  - dsa_detach: Detaches from dynamic shared area
  - dsa_release_in_place: Manually releases DSA reference count
- Called from (representative examples):
  - pgstat_shutdown_hook: Called during backend shutdown process

## Notes and Other Information
- Contains assertion to ensure DSA is attached before attempting detach
- Releases all entry references before detaching to prevent resource leaks
- Sets pgStatLocal.shared_hash to NULL after detaching from hash table
- Manually calls dsa_release_in_place() because dsa_detach() doesn't decrement reference count when no segment was provided to dsa_attach_in_place()
- Sets pgStatLocal.dsa to NULL after cleanup to indicate detached state
- Part of the backend shutdown sequence for proper resource cleanup
- Prevents leaving dangling references to shared statistics structures