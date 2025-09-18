# decide_file_actions

## Location
src/bin/pg_rewind/filemap.c: 861 - 892

## Overview
Processes all file entries in the global filehash to determine appropriate actions and returns a sorted filemap ready for execution during pg_rewind operations.

## Definition
```c
filemap_t *decide_file_actions(void)
```

## Detailed Description
This function serves as the main entry point for the file action decision phase of pg_rewind. It performs a complete transformation of the file tracking system from a hash-based structure used during discovery to a sorted array ready for execution.

The function operates in three phases:
1. **Decision phase**: Iterates through all entries in the global filehash and calls decide_file_action() to determine the appropriate action for each file
2. **Collection phase**: Converts the hash table entries into a linear array within a newly allocated filemap_t structure  
3. **Sorting phase**: Uses qsort with final_filemap_cmp to order entries for safe execution

The resulting filemap contains entries sorted in the order that their actions should be executed, ensuring that:
- Directory creation happens before file creation within those directories
- File and subdirectory removal happens before parent directory removal
- Operations follow a predictable, safe sequence

## Parameters / Member Variables
- None (operates on global filehash)

## Dependencies
- Functions called/Symbols referenced:
  - filehash_start_iterate
  - filehash_iterate  
  - [decide_file_action](decide_file_action.md)
  - pg_malloc
  - qsort
  - [final_filemap_cmp](../f/final_filemap_cmp.md)
  - filehash (global variable)
  - [filemap_t](../f/filemap_t.md) (return type)
  - [file_entry_t](../f/file_entry_t.md) (array element type)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_rewind.c)

## Notes and Other Information
- This is a public function (not static) that serves as the main API for the file decision subsystem
- The function allocates memory for the filemap using pg_malloc with a flexible array member pattern
- The returned filemap must be freed by the caller
- The function assumes that the global filehash has been populated with file information from both source and target systems
- After this function completes, the filemap is ready for execution by other parts of pg_rewind
- The nentries field in the returned filemap equals the number of members in the input filehash