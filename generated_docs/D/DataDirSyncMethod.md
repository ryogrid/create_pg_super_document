# DataDirSyncMethod

## Location
[src/include/common/file_utils.h:31-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/file_utils.h#L31-L61)

## Overview
DataDirSyncMethod is an enumeration that specifies different methods for synchronizing data directory contents to persistent storage during PostgreSQL operations.

## Definition

```c
struct iovec;
```
## Detailed Description
The DataDirSyncMethod enumeration defines synchronization strategies used by PostgreSQL utilities and operations to ensure data durability by forcing writes to persistent storage. This is critical for backup operations, base backup creation, and data directory synchronization. The enum provides platform-specific sync methods with fsync being the traditional approach and syncfs being a more efficient Linux-specific alternative when available.

## Parameters / Member Variables
- `DATA_DIR_SYNC_METHOD_FSYNC`: Uses individual fsync() calls on each file to ensure data is written to storage
- `DATA_DIR_SYNC_METHOD_SYNCFS`: Uses syncfs() system call to sync an entire filesystem (Linux-specific, more efficient for large operations)

## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Called from (representative examples):
  - [sync_pgdata](../s/sync_pgdata.md) at src/common/file_utils.c:99
  - [sync_dir_recurse](../s/sync_dir_recurse.md) at src/common/file_utils.c:220
  - [parse_sync_method](../p/parse_sync_method.md) at src/fe_utils/option_utils.c:90
  - Used in pg_basebackup, pg_combinebackup, pg_upgrade utilities
  - Referenced in function signatures at src/include/common/file_utils.h:38-39

## Notes and Other Information
This enumeration is used primarily in PostgreSQL utility programs that need to ensure data durability during operations like backup creation and data directory synchronization. The syncfs method is only available on Linux systems with HAVE_SYNCFS defined and provides better performance for large-scale synchronization operations. The parse_sync_method() function in src/fe_utils/option_utils.c handles the conversion from command-line string options ("fsync" or "syncfs") to the corresponding enum values, with appropriate fallbacks and error handling for unsupported sync methods.