# calculate_totals

## Location
[src/bin/pg_rewind/filemap.c:499-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L499-L539)

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
  - [filemap_t](../f/filemap_t.md) (struct type)
  - [file_entry_t](../f/file_entry_t.md) (struct type)
  - FILE_TYPE_REGULAR
  - FILE_ACTION_COPY
  - FILE_ACTION_COPY_TAIL
  - [datapagemap_iterator_t](../d/datapagemap_iterator_t.md)
  - [datapagemap_iterate](../d/datapagemap_iterate.md)
  - [datapagemap_next](../d/datapagemap_next.md)
  - [pg_free](../p/pg_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_rewind.c)

## Notes and Other Information
- The calculated totals are stored directly in the filemap structure's total_size and fetch_size fields
- Used for displaying progress information to users during the rewind operation
- The fetch_size calculation accounts for partial file copies and individual page overwrites to provide accurate progress estimates
- Part of the pg_rewind utility's file synchronization system
- BLCKSZ constant is used to calculate the size contribution of individual database pages

## Simplified Source

```c
void calculate_totals(filemap_t *filemap)
{
    file_entry_t *entry;
    int i;

    // Initialize totals
    filemap->total_size = 0;
    filemap->fetch_size = 0;

    // Process each file entry in the filemap
    for (i = 0; i < filemap->nentries; i++)
    {
        entry = filemap->entries[i];

        // Only process regular files (skip directories, symlinks, etc.)
        if (entry->source_type != FILE_TYPE_REGULAR)
            continue;

        // Add to total size of all source files
        filemap->total_size += entry->source_size;

        // Calculate fetch size based on action type
        if (entry->action == FILE_ACTION_COPY)
        {
            // Copy entire file
            filemap->fetch_size += entry->source_size;
            continue;
        }

        if (entry->action == FILE_ACTION_COPY_TAIL)
        {
            // Copy only the portion beyond target file size
            filemap->fetch_size += (entry->source_size - entry->target_size);
        }

        // Add size for individual pages that need to be overwritten
        if (entry->target_pages_to_overwrite.bitmapsize > 0)
        {
            datapagemap_iterator_t *page_iterator;
            BlockNumber block_number;

            page_iterator = datapagemap_iterate(&entry->target_pages_to_overwrite);
            while (datapagemap_next(page_iterator, &block_number))
                filemap->fetch_size += BLCKSZ;

            pg_free(page_iterator);
        }
    }
}
```