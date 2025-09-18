# tuplesort_attach_shared

## Location
src/backend/utils/sort/tuplesort.c: 2999 - 3018

## Overview
Attaches worker processes to the shared tuplesort state that was previously initialized by the leader process.

## Definition
```c
void tuplesort_attach_shared(Sharedsort *shared, dsm_segment *seg)
```

## Detailed Description
This function allows worker processes to connect to the shared tuple sorting infrastructure that was set up by the leader process using tuplesort_initialize_shared(). It establishes the necessary connections to shared file sets and other shared resources required for parallel sorting operations. This is a lightweight operation that primarily handles the attachment to shared file systems.

## Parameters / Member Variables
- `shared`: Pointer to the Sharedsort structure containing the shared state for parallel sorting
- `seg`: DSM (Dynamic Shared Memory) segment used for shared file operations

## Dependencies
- Functions called/Symbols referenced:
  - SharedFileSetAttach
  - dsm_segment
  - Sharedsort
- Called from (representative examples):
  - _brin_parallel_build_main
  - _bt_parallel_build_main

## Notes and Other Information
- Must be called by all worker processes after tuplesort_initialize_shared() has been called by the leader
- This is a companion function to tuplesort_initialize_shared() - the leader initializes, workers attach
- The function primarily handles SharedFileSet attachment for coordinated file operations across processes
- Essential for establishing the shared infrastructure needed for parallel tuple sorting