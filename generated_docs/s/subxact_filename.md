# subxact_filename

## Location
src/backend/replication/logical/worker.c: 4197 - 4203

## Overview
Generates a standardized filename for storing subtransaction information files in logical replication.

## Definition
```c
static inline void subxact_filename(char *path, Oid subid, TransactionId xid)
```

## Detailed Description
This inline function creates a standardized filename format for subtransaction information files used in PostgreSQL's logical replication system. The filename format follows the pattern "{subscription_id}-{transaction_id}.subxacts", ensuring unique identification of subtransaction data files for each combination of subscription and toplevel transaction. This naming scheme allows for efficient file management and cleanup operations.

## Parameters / Member Variables
- `path`: Output buffer to store the generated filename (must be at least MAXPGPATH size)
- `subid`: Object ID of the subscription
- `xid`: Transaction ID of the toplevel transaction

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C library function)
  - MAXPGPATH (PostgreSQL constant)
- Called from (representative examples):
  - subxact_info_write
  - subxact_info_read
  - stream_cleanup_files

## Notes and Other Information
- This is a static inline function for optimal performance given its frequent usage
- The filename format is "{subid}-{xid}.subxacts"
- The .subxacts extension clearly identifies the file type and purpose
- The function assumes the caller provides a buffer of sufficient size (MAXPGPATH)
- This naming scheme ensures unique filenames for each subscription-transaction combination
- The function is used consistently across all subtransaction file operations for naming uniformity
- No error checking is performed on buffer size - caller responsibility to provide adequate buffer