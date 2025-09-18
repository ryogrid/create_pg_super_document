# print_filemap

## Location
[src/bin/pg_rewind/filemap.c:540-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L540-L569)

## Overview
Prints debugging information about the filemap showing which files will be processed and what actions will be taken during pg_rewind execution.

## Definition
void print_filemap(filemap_t *filemap)

## Detailed Description
This function iterates through all entries in a filemap structure and prints debugging information for files that require some action during the rewind process. It only prints information for files that either:

1. Have a file action other than FILE_ACTION_NONE, or 
2. Have pages that need to be overwritten (indicated by target_pages_to_overwrite.bitmapsize > 0)

For each qualifying file entry, it logs:
- The file path
- The action to be performed (using action_to_str to convert the action enum to a readable string)
- If applicable, detailed information about which specific pages need to be overwritten (via datapagemap_print)

The function concludes by flushing stdout to ensure all debug output is immediately visible to the user.

## Parameters / Member Variables
- filemap: Pointer to the filemap_t structure containing the list of files and their associated actions

## Dependencies
- Functions called/Symbols referenced:
  - [filemap_t](../f/filemap_t.md) (struct type)
  - [file_entry_t](../f/file_entry_t.md) (struct type)
  - FILE_ACTION_NONE
  - pg_log_debug
  - [action_to_str](../a/action_to_str.md)
  - [datapagemap_print](../d/datapagemap_print.md)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_rewind.c)

## Notes and Other Information
- This is primarily a debugging and informational function used when pg_rewind runs in verbose mode
- Only prints information for files that require some action, filtering out files with FILE_ACTION_NONE that don't have specific pages to overwrite
- The datapagemap_print output provides detailed information about which specific database pages need to be updated
- Part of pg_rewind's diagnostic and troubleshooting capabilities
- Uses pg_log_debug so output level can be controlled by logging configuration