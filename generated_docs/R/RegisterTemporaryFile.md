# RegisterTemporaryFile

## Location
[src/backend/storage/file/fd.c:1544-1558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1544-L1558)

## Overview
RegisterTemporaryFile is a static function that registers a temporary file with the resource management system for automatic cleanup at transaction end.

## Definition

```c
static void
RegisterTemporaryFile(File file)
```
## Detailed Description
RegisterTemporaryFile registers a temporary file with PostgreSQL's resource management system to ensure proper cleanup. The function implements a two-level cleanup strategy:

1. **Resource Owner Tracking**: Associates the file with the current resource owner (CurrentResourceOwner), which will ensure the file is closed when the resource owner is released or reset.

2. **Transaction-level Backup**: Sets the FD_CLOSE_AT_EOXACT flag as a backup mechanism to ensure the file is closed at the end of the current transaction, even if the resource owner cleanup fails.

This dual approach provides robust cleanup guarantees for temporary files, preventing resource leaks in various scenarios including error conditions, transaction aborts, and normal transaction completion.

The function also sets a global flag (have_xact_temporary_files) to indicate that the current transaction has temporary files that need cleanup.

## Parameters / Member Variables
- : The File (virtual file descriptor) of the temporary file to register

## Dependencies
- Functions called/Symbols referenced:
  - File (type definition for virtual file descriptor)
  - ResourceOwnerRememberFile (function to register file with resource owner)
  - FD_CLOSE_AT_EOXACT (flag constant for transaction-end cleanup)
- Called from (representative examples):
  - OpenTemporaryFile
  - PathNameCreateTemporaryFile
  - PathNameOpenTemporaryFile

## Notes and Other Information
- This is a static function internal to fd.c, not exposed in the public API
- Requires that ResourceOwnerEnlarge(CurrentResourceOwner) was called before the file was opened
- Implements a dual cleanup strategy for robust resource management
- The resource owner mechanism provides hierarchical cleanup (e.g., for subtransactions)
- The FD_CLOSE_AT_EOXACT flag serves as a backup cleanup mechanism
- Setting have_xact_temporary_files to true triggers transaction-end cleanup routines
- Essential for preventing temporary file leaks in PostgreSQL's transaction system
- Part of PostgreSQL's comprehensive resource management and cleanup infrastructure