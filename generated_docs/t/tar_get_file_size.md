# tar_get_file_size

## Location
[src/bin/pg_basebackup/walmethods.c:1007-1016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1007-L1016)

## Overview
A placeholder function in the TAR-based WAL method implementation that is currently not implemented and always returns an error.

## Definition

```c
static ssize_t
tar_get_file_size(WalWriteMethod *wwmethod, const char *pathname)
```
## Detailed Description
This function is part of the TAR-based WAL writing method infrastructure in pg_basebackup. It serves as a placeholder for getting the size of a file within the TAR method context. The function is currently not implemented and immediately returns an error with errno set to ENOSYS (function not implemented).

The function follows the WalWriteMethod interface pattern but provides no actual functionality, indicating that file size retrieval is not currently needed or supported in the TAR-based WAL method.

## Parameters / Member Variables
- : Pointer to the WalWriteMethod structure representing the TAR-based WAL writing method
- : The path name of the file whose size is to be retrieved (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - [WalWriteMethod](../W/WalWriteMethod.md) (structure type)
- Called from (representative examples):
  - [CreateWalDirectoryMethod](../C/CreateWalDirectoryMethod.md) (function pointer assignment)

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- The function sets lasterrno to ENOSYS and returns -1 to indicate the operation is not supported
- The comment indicates this functionality is "Currently not used, so not supported"
- This is part of the function pointer interface for WalWriteMethod operations