# cleanup_output_dirs

## Location
[src/bin/pg_upgrade/util.c:63-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L63-L128)

## Overview
Removes internally generated log files and directories during pg_upgrade exit cleanup, with intelligent handling of directory states.

## Definition

```c
void
cleanup_output_dirs(void)
```
## Detailed Description
The  function performs cleanup operations when pg_upgrade exits, managing the removal of temporary logs and directories created during the upgrade process. The function implements a sophisticated cleanup strategy:

1. **Internal log closure**: First closes the internal log file handle
2. **Retention check**: Respects the  setting - if retention is enabled, no cleanup occurs
3. **Base directory cleanup**: Attempts to remove the base log directory twice (for Windows file unlinking delays)
4. **Root directory intelligent cleanup**: Uses  to determine the state of the root output directory and acts accordingly:
   - Empty directories or those containing only dot files are removed
   - Directories containing previous log activity are preserved
   - Mount points trigger assertions
   - Access errors are logged as warnings

The double-removal approach addresses Windows-specific timing issues where files may still be in the process of being unlinked.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  (closes log_opts.internal file handle)
  -  (recursive directory removal)
  -  (directory state checking)
  -  (logging with PG_WARNING level)
  -  (global retain setting)
  -  (base directory path)
  -  (root directory path)
  -  (warning log level constant)
- Called from (representative examples):
  -  in src/bin/pg_upgrade/check.c:730
  -  in src/bin/pg_upgrade/pg_upgrade.c:238

## Notes and Other Information
- Part of pg_upgrade's cleanup infrastructure, typically called during program termination
- Implements platform-specific workarounds for Windows file system behavior
- Preserves directories containing historical log data while cleaning temporary files
- Uses Assert() statements for unexpected directory states (non-existent or mount points)
- The function is designed to be safe to call multiple times and handles various error conditions gracefully