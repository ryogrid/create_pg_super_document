# tar_existsfile

## Location
[src/bin/pg_basebackup/walmethods.c:1219-1226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L1219-L1226)

## Overview
A function that checks if a file exists within a TAR-based WAL method, always returning false since TAR methods only create new files.

## Definition
```c
static bool tar_existsfile(WalWriteMethod *wwmethod, const char *pathname)
```

## Detailed Description
This function is part of the TAR-based WAL writing method infrastructure in pg_basebackup. It implements the "exists file" check operation for the TAR method. However, since the TAR method is designed to create new TAR files from scratch and doesn't support checking for existing files within existing TAR archives, this function always returns false.

The function serves as a concrete implementation of the WalWriteMethod interface but provides no actual file existence checking functionality. This design choice reflects the TAR method's approach of creating new archives rather than modifying existing ones.

## Parameters / Member Variables
- `wwmethod`: Pointer to the WalWriteMethod structure representing the TAR-based WAL writing method
- `pathname`: The path name of the file to check for existence (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - clear_error
  - WalWriteMethod (structure type)
- Called from (representative examples):
  - CreateWalDirectoryMethod (function pointer assignment)

## Notes and Other Information
- This function is marked as static, meaning it's only accessible within the walmethods.c file
- Always returns false, indicating that no externally created files exist in the TAR method context
- The comment explains the design rationale: "We only deal with new tarfiles, so nothing externally created exists"
- Clears any previous error state before returning
- This is part of the function pointer interface for WalWriteMethod operations
- The pathname parameter is effectively ignored since the function always returns false