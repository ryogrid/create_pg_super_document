# datapagemap_print

## Location
src/bin/pg_rewind/datapagemap.c: 117 - 127

## Overview
A debugging utility function that prints all block numbers marked in a datapagemap bitmap to the debug log.

## Definition
void datapagemap_print(datapagemap_t *map)

## Detailed Description
This function provides a debugging aid for examining the contents of a datapagemap bitmap. It creates an iterator for the provided map and uses the standard iteration pattern (datapagemap_iterate and datapagemap_next) to traverse all set bits. For each set bit found, it logs the corresponding block number using pg_log_debug. The function properly cleans up by freeing the iterator after use. This is primarily used for development and debugging purposes to visualize which blocks are marked in the bitmap.

## Parameters / Member Variables
- : Pointer to the datapagemap_t structure to print

## Dependencies
- Functions called/Symbols referenced:
  - [datapagemap_iterate](datapagemap_iterate.md) (to create the iterator)
  - [datapagemap_next](datapagemap_next.md) (to traverse set bits)
  - pg_log_debug (for debug output)
  - [pg_free](../p/pg_free.md) (to clean up the iterator)
- Called from (representative examples):
  - [print_filemap](../p/print_filemap.md) (in filemap.c:555)

## Notes and Other Information
- Used for debugging and development purposes only
- Outputs to debug log level, so visibility depends on logging configuration
- Follows the standard iterator pattern with proper cleanup
- Part of the pg_rewind utility's debugging infrastructure for bitmap visualization
- The output format shows each block number on a separate debug log line