# decide_file_action

## Location
src/bin/pg_rewind/filemap.c: 700 - 860

## Overview
Determines the appropriate action to perform on a file during pg_rewind operation by analyzing file existence, type, and content differences between source and target systems.

## Definition
```c
static file_action_t decide_file_action(file_entry_t *entry)
```

## Detailed Description
This function implements the core decision logic for pg_rewind file handling. It examines a file entry containing information about a file's status on both source and target systems and determines what action should be taken. The decision process follows this hierarchy:

1. **Special file handling**: Skip control files and system files that shouldn't be modified
2. **Exclusion filtering**: Remove files matching exclusion patterns from the target
3. **Existence-based decisions**: Handle cases where files exist on only one system
4. **Type compatibility**: Ensure files are of the same type on both systems
5. **Content-based decisions**: For regular files, determine if copying, truncating, or no action is needed

For relation data files, the function implements sophisticated logic to handle size differences:
- If target is smaller than source: copy the missing tail (FILE_ACTION_COPY_TAIL)
- If target is larger than source: truncate to source size (FILE_ACTION_TRUNCATE)  
- If sizes are equal: no action needed (FILE_ACTION_NONE)

## Parameters / Member Variables
- `entry`: Pointer to file_entry_t structure containing file information for both source and target systems

## Dependencies
- Functions called/Symbols referenced:
  - check_file_excluded
  - keepwal_entry_exists
  - pg_log_debug
  - pg_str_endswith
  - pg_fatal
  - FILE_ACTION_* constants
  - FILE_TYPE_* constants
- Called from (representative examples):
  - decide_file_actions

## Notes and Other Information
- This is a static function internal to filemap.c
- The function handles the pg_control file specially, leaving it for later processing
- macOS .DS_Store files are explicitly ignored
- The function includes safety checks and will call pg_fatal for unexpected conditions
- For relation files, the logic assumes that WAL replay will handle block-level changes, so only size differences need to be addressed
- The keepwal hash table is consulted to preserve files needed for recovery