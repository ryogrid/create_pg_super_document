# extension_file_exists

## Location
[src/backend/commands/extension.c:2260-2312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2260-L2312)

## Overview
This function tests whether a given extension exists by checking for the presence of its primary control file in the extension control directory.

## Definition


## Detailed Description
The extension_file_exists function provides a lightweight way to check if an extension is available on the system (not whether it's installed in the current database). It works by scanning the extension control directory for a primary control file matching the given extension name.

The function performs the following steps:
1. Gets the extension control directory path
2. Opens and scans the directory for control files
3. For each valid control file found, extracts the extension name
4. Compares the extracted name with the requested extension name
5. Returns true if a match is found, false otherwise

The function specifically ignores auxiliary control files (those containing "--" in the filename) and only considers primary control files. This check is not bulletproof since the control file content might be invalid, but it's sufficient for hint purposes where 100% accuracy isn't required.

## Parameters / Member Variables
- : The name of the extension to check for existence (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_control_directory](../g/get_extension_control_directory.md)
  - AllocateDir
  - ReadDir
  - [is_extension_control_filename](../i/is_extension_control_filename.md)
  - [pstrdup](../p/pstrdup.md)
  - strrchr
  - strstr
  - strcmp
  - FreeDir
- Called from (representative examples):
  - [CreateFunction](../C/CreateFunction.md) (in functioncmds.c)
  - [ExecuteDoStmt](../E/ExecuteDoStmt.md) (in functioncmds.c)

## Notes and Other Information
- This function is designed for hint/suggestion purposes and doesn't guarantee the extension is fully valid
- Returns false silently if the extension control directory doesn't exist
- Only checks for primary control files, ignoring version-specific auxiliary control files
- The function is used during function creation and DO statement execution to provide helpful error messages
- Performance consideration: The function scans the entire control directory on each call, so it's not optimized for frequent use
- The check is filename-based only - it doesn't validate the control file contents