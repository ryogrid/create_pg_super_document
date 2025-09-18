# filemap_t

## Location
src/bin/pg_rewind/filemap.h: 89 - 97

## Overview
The filemap_t structure contains the final decisions and summary information for all file operations to be performed during a PostgreSQL pg_rewind process.

## Definition


## Detailed Description
The filemap_t structure serves as the master container for all file operation decisions made during pg_rewind execution. It contains an array of file_entry_t pointers sorted in the order that their actions should be executed, along with summary statistics about the total work to be performed. This structure is created after all file information has been gathered and analyzed, representing the final plan for synchronizing the target cluster with the source cluster.

## Parameters / Member Variables
- : The total size in bytes of all files in the source cluster
- : The number of bytes that need to be copied from source to target during the rewind operation
- : The number of entries in the entries array
- : Flexible array member containing pointers to file_entry_t structures, sorted by execution order

## Dependencies
- Functions called/Symbols referenced:
  - file_entry_t (structure for individual file information)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)
- Called from (representative examples):
  - calculate_totals (computes summary statistics)
  - print_filemap (displays the filemap contents)
  - decide_file_actions (creates and populates the filemap)
  - perform_rewind (executes the actions defined in the filemap)

## Notes and Other Information
This structure represents the culmination of pg_rewind's analysis phase, containing the complete execution plan for file synchronization. The entries array is sorted to ensure operations are performed in the correct order (e.g., removing files before creating directories). The summary fields (total_size, fetch_size) are used for progress reporting and validation. The structure uses a flexible array member to efficiently store a variable number of file entry pointers.