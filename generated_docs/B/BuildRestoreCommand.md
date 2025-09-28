# BuildRestoreCommand

## Location
[src/common/archive.c:39-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/archive.c#L39-L60)

## Overview
Builds a restore command to retrieve a file from WAL archives by replacing supported aliases with values supplied by the caller as defined by the GUC parameter restore_command.

## Definition

```c
char *
BuildRestoreCommand(const char *restoreCommand,
					const char *xlogpath,
					const char *xlogfname,
					const char *lastRestartPointFname)
```
## Detailed Description
The  function constructs a shell command string used to restore WAL (Write-Ahead Log) files from archive storage during PostgreSQL recovery operations. This function processes a template restore command string and replaces percent placeholders with actual values needed for the restoration process.

The function supports three specific placeholder replacements:
-  is replaced with  (the path where the restored file should be placed)
-  is replaced with  (the WAL filename to be restored)  
-  is replaced with  (filename of the last restart point)

The function handles path normalization on Windows platforms by converting Unix-style forward slashes to Windows-style backslashes in the xlogpath parameter. This ensures compatibility with Windows shell commands and utilities.

Error handling is built into the function through the  helper - if any required argument is NULL and the corresponding placeholder is found in the restore command, an error will be thrown.

## Parameters / Member Variables
- : Template string containing the restore command with percent placeholders to be replaced
- : File system path where the restored WAL file should be placed (replaces %p placeholder)  
- : Name of the WAL file to be restored from the archive (replaces %f placeholder)
- : Filename of the last restart point WAL file (replaces %r placeholder)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a duplicate copy of the xlogpath string
  -  - Converts Unix-style paths to native format (Windows backslashes)
  -  - Core function that performs the placeholder substitutions
  -  - Frees the allocated native path string
- Called from (representative examples):
  -  (in src/backend/access/transam/xlogarchive.c:153)
  -  (in src/fe_utils/archive.c:49)

## Notes and Other Information
- The function returns a palloc'd string that the caller is responsible for freeing
- [Path](../P/Path.md) conversion to native format only occurs on Windows platforms (#ifdef WIN32)
- The placeholder replacement follows the pattern "frp" meaning %f, %r, %p in that parameter order
- This function is part of PostgreSQL's WAL archiving and recovery infrastructure
- The restore command is typically configured via the  GUC parameter
- Used during point-in-time recovery and standby server operations to retrieve archived WAL files
- File location: src/common/archive.c:39-60

## Simplified Source

```c
// Simplified version of BuildRestoreCommand
char *
BuildRestoreCommand(const char *restoreCommand,
                    const char *xlogpath,
                    const char *xlogfname,
                    const char *lastRestartPointFname)
{
    char *nativePath = NULL;
    char *result;

    // Convert xlogpath to native format if provided (Windows compatibility)
    if (xlogpath) {
        nativePath = pstrdup(xlogpath);
        make_native_path(nativePath);
    }

    // Replace placeholders: %f with xlogfname, %r with lastRestartPointFname, %p with nativePath
    result = replace_percent_placeholders(restoreCommand, "restore_command", "frp",
                                          xlogfname, lastRestartPointFname, nativePath);

    // Clean up temporary native path copy
    if (nativePath)
        pfree(nativePath);

    return result;
}
```

Key simplifications made:
- Added descriptive comments explaining the core logic steps
- Clarified the placeholder replacement pattern (%f, %r, %p)
- Explained the Windows path conversion purpose
- Maintained the essential algorithm flow and error handling through the helper function
- Preserved memory management (pstrdup/pfree) which is critical for correctness