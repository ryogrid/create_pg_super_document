# BaseBackupGetTargetHandle

## Location
[src/backend/backup/basebackup_target.c:117-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L117-L162)

## Overview
Looks up a registered base backup target by name, validates the target-specific configuration details, and returns a handle for subsequent backup operations.

## Definition
```c
BaseBackupTargetHandle *BaseBackupGetTargetHandle(char *target, char *target_detail)
```

## Detailed Description
This function searches the global list of registered backup target types for a match with the provided target name. When a match is found, it creates a new BaseBackupTargetHandle structure containing a reference to the target type and the processed target details. The target-specific details are validated and processed by calling the target type's check_detail function, which can perform validation, parsing, or other preprocessing tasks.

If no matching target is found, the function reports an error with ERRCODE_FEATURE_NOT_SUPPORTED. The function ensures the target list is initialized before searching, making it safe to call even if no targets have been registered yet.

## Parameters / Member Variables
- `target`: Name of the backup target type to look up (e.g., "client", "server-file")
- `target_detail`: Target-specific configuration string that will be validated and processed by the target type's check_detail function

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_target_list](../i/initialize_target_list.md)
  - [BaseBackupTargetType](BaseBackupTargetType.md) (struct)
  - [BaseBackupTargetHandle](BaseBackupTargetHandle.md) (struct)
  - [palloc](../p/palloc.md)
  - strcmp
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)

- Called from (representative examples):
  - [parse_basebackup_options](../p/parse_basebackup_options.md)

## Notes and Other Information
- Part of the base backup target validation and setup process
- The check_detail function is called to validate and process target-specific parameters
- Returns a handle that contains both the target type information and processed detail arguments
- Throws an error if the requested target type is not registered
- The returned handle is used by BaseBackupGetSink to create the actual backup sink
- Memory for the handle is allocated in the current memory context