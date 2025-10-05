# sts_filename

## Location
[src/backend/utils/sort/sharedtuplestore.c:598-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L598-L601)

## Overview
Generates the filename for a participant's temporary file in a shared tuple store, providing unique file naming for each parallel worker process.

## Definition

```c
static void
sts_filename(char *name, SharedTuplestoreAccessor *accessor, int participant)
```
## Detailed Description
This is a simple utility function that constructs standardized filenames for shared tuple store temporary files. Each participant in a parallel operation needs its own temporary file to store tuples, and this function ensures consistent naming across the system. The filename format follows the pattern "[base_name].p[participant_number]", where the base name comes from the shared tuple store's name field and the participant number uniquely identifies each parallel worker.

This naming convention allows the shared tuple store system to:
- Create unique files for each participant in parallel operations
- Easily identify which participant owns which temporary file
- Maintain organized file management within the PostgreSQL temporary file system

## Parameters / Member Variables
- `*name`: Output buffer to store the generated filename (must be at least MAXPGPATH bytes)
- `*accessor`: SharedTuplestoreAccessor containing the shared tuple store with base name information
- `participant`: Integer identifier of the participant (parallel worker) for which to generate the filename
## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for formatted string creation
  - : PostgreSQL constant defining maximum path length
  - : Struct providing access to shared tuple store state
- Called from (representative examples):
  - : When creating temporary files for writing tuples
  - : When opening temporary files for reading tuples during parallel scans
  - General shared tuple store operations requiring file access

## Notes and Other Information
- This function is declared static, limiting its scope to the sharedtuplestore.c file
- The function assumes the output buffer is pre-allocated with sufficient size (MAXPGPATH)
- The naming convention ensures no filename collisions between different participants
- Files created with these names are temporary and managed by PostgreSQL's buffer file system
- The participant numbering starts from 0 and corresponds to the parallel worker process index
- Used extensively in parallel hash join operations where tuple redistribution requires temporary storage

## Simplified Source

```c
static void sts_filename(char *name, SharedTuplestoreAccessor *accessor, int participant) {
    // Generate filename: [base_name].p[participant_number]
    snprintf(name, MAXPGPATH, "%s.p%d", accessor->sts->name, participant);
}
```