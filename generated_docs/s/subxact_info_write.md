# subxact_info_write

## Location
src/backend/replication/logical/worker.c: 4019 - 4067

## Overview
Stores information about subtransactions for a toplevel transaction to a file in the logical replication worker's fileset.

## Definition
```c
static void subxact_info_write(Oid subid, TransactionId xid)
```

## Detailed Description
This function manages the persistent storage of subtransaction information for logical replication workers. It writes subtransaction data to a dedicated file, storing the offset of each subtransaction's first change in the main stream file. The function handles both creation of new files and deletion of existing ones when no subtransactions remain. The entire file is overwritten as a complete unit rather than incrementally updated.

## Parameters / Member Variables
- `subid`: Object ID of the subscription
- `xid`: Transaction ID of the toplevel transaction

## Dependencies
- Functions called/Symbols referenced:
  - subxact_filename
  - cleanup_subxact_info
  - BufFileDeleteFileSet
  - BufFileOpenFileSet
  - BufFileCreateFileSet
  - BufFileWrite
  - BufFileClose
  - SubXactInfo
  - BufFile
- Called from (representative examples):
  - stream_stop_internal
  - stream_abort_internal

## Notes and Other Information
- This is a static function with internal linkage within worker.c
- The function uses PostgreSQL's BufFile system for file I/O operations
- Files are managed within the logical replication worker's stream fileset
- When no subtransactions exist (nsubxacts == 0), the file is deleted
- The implementation includes a TODO comment noting that only non-aborted subtransactions should be stored
- Memory allocated for subtransaction info is freed after writing via cleanup_subxact_info()
- The file format includes a count followed by the actual SubXactInfo structures