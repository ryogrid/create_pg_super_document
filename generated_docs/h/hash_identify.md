# hash_identify

## Location
[src/backend/access/rmgrdesc/hashdesc.c:126-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/hashdesc.c#L126-L173)

## Overview
The hash_identify function maps hash index WAL record type codes to their corresponding human-readable string identifiers.

## Definition
const char *hash_identify(uint8 info)

## Detailed Description
This function serves as a lookup mechanism that translates numeric WAL record type identifiers for hash index operations into descriptive string names. It is part of PostgreSQL's WAL record identification framework, enabling tools and debugging utilities to display meaningful operation names instead of raw numeric codes. The function examines the info parameter (after masking out non-record-type bits) and returns the appropriate string identifier for the hash index operation type.

The function handles all major hash index WAL record types including metadata operations, tuple manipulation, bucket splitting, page management, and maintenance operations.

## Parameters / Member Variables
- `info`: uint8 value containing the WAL record type information, with info bits that need to be masked out

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK (for masking info bits)
- WAL record types identified:
  - XLOG_HASH_INIT_META_PAGE -> "INIT_META_PAGE"
  - XLOG_HASH_INIT_BITMAP_PAGE -> "INIT_BITMAP_PAGE" 
  - XLOG_HASH_INSERT -> "INSERT"
  - XLOG_HASH_ADD_OVFL_PAGE -> "ADD_OVFL_PAGE"
  - XLOG_HASH_SPLIT_ALLOCATE_PAGE -> "SPLIT_ALLOCATE_PAGE"
  - XLOG_HASH_SPLIT_PAGE -> "SPLIT_PAGE"
  - XLOG_HASH_SPLIT_COMPLETE -> "SPLIT_COMPLETE"
  - XLOG_HASH_MOVE_PAGE_CONTENTS -> "MOVE_PAGE_CONTENTS"
  - XLOG_HASH_SQUEEZE_PAGE -> "SQUEEZE_PAGE"
  - XLOG_HASH_DELETE -> "DELETE"
  - XLOG_HASH_SPLIT_CLEANUP -> "SPLIT_CLEANUP"
  - XLOG_HASH_UPDATE_META_PAGE -> "UPDATE_META_PAGE"
  - XLOG_HASH_VACUUM_ONE_PAGE -> "VACUUM_ONE_PAGE"
- Called from (representative examples):
  - SizeOfHashVacuumOnePage

## Notes and Other Information
- Returns NULL for unrecognized record types
- The function masks out info bits using XLR_INFO_MASK to isolate the record type
- [String](../S/String.md) identifiers are concise and descriptive, suitable for logging and debugging output
- Part of PostgreSQL's resource manager identification framework for hash indexes
- Commonly used by WAL analysis tools and debugging utilities