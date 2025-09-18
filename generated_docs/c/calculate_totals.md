# calculate_totals

## Location
src/bin/pg_rewind/filemap.c: 499 - 539

## Overview
Calculates the total size of files to be processed and the total size of data to be fetched for progress reporting during pg_rewind operations.

## Definition
void calculate_totals(filemap_t *filemap)

## Detailed Description
This function iterates through all entries in a filemap structure to compute two key metrics used for progress reporting in pg_rewind:

1. **total_size**: The sum of all source file sizes for regular files in the filemap
2. **fetch_size**: The amount of data that actually needs to be fetched/copied from the source

The function processes different file actions differently:
- For FILE_ACTION_COPY: adds the entire source file size to fetch_size
- For FILE_ACTION_COPY_TAIL: adds only the portion that needs to be copied (source_size - target_size)
- For files with target_pages_to_overwrite: calculates the exact number of pages that need to be fetched using the datapagemap

Only regular files (FILE_TYPE_REGULAR) are included in the calculations, as directories, symlinks, and other special files don't contribute to the data transfer size.

## Parameters / Member Variables
- filemap: Pointer to the filemap_t structure containing the list of files and their associated actions

## Dependencies
- Functions called/Symbols referenced:
  - filemap_t (struct type)
  - file_entry_t (struct type)
  - FILE_TYPE_REGULAR
  - FILE_ACTION_COPY
  - FILE_ACTION_COPY_TAIL
  - datapagemap_iterator_t
  - datapagemap_iterate
  - datapagemap_next
  - pg_free
- Called from (representative examples):
  - main (in pg_rewind.c)

## Notes and Other Information
- The calculated totals are stored directly in the filemap structure's total_size and fetch_size fields
- Used for displaying progress information to users during the rewind operation
- The fetch_size calculation accounts for partial file copies and individual page overwrites to provide accurate progress estimates
- Part of the pg_rewind utility's file synchronization system
- BLCKSZ constant is used to calculate the size contribution of individual database pages