# warn_on_mount_point

## Location
src/bin/initdb/initdb.c: 3016 - 3028

## Overview
Provides specific warnings and guidance when initdb detects that a directory appears to be a filesystem mount point, helping users avoid potential issues with using mount points directly as PostgreSQL data directories.

## Definition


## Detailed Description
This function generates informative warnings when  detects conditions that suggest a directory is a filesystem mount point. It provides context-specific error details and guidance to help users understand why using mount points directly as PostgreSQL data directories is problematic.

The function handles two specific mount point indicators:
1. **Dot-prefixed/invisible files (error code 2)**: Often created by filesystem utilities or mount processes
2. **lost+found directory (error code 3)**: Typically present in the root of ext2/ext3/ext4 filesystems

After providing the specific error detail, it always includes a hint recommending the creation of a subdirectory under the mount point instead of using the mount point root directly.

## Parameters / Member Variables
- : Integer error code from  indicating the type of mount point condition detected
  - : Directory contains dot-prefixed/invisible files
  - : Directory contains a lost+found directory

## Dependencies
- Functions called/Symbols referenced:
  - : Logs detailed error information about the specific mount point condition
  - : Provides helpful guidance on how to resolve the issue
- Called from (representative examples):
  - : Called when data directory appears to be a mount point
  - : Called when external WAL directory appears to be a mount point

## Notes and Other Information
- This function is part of initdb's defensive design to prevent common configuration mistakes
- Using mount points directly as data directories can lead to various issues including permission problems, performance issues, and data loss risks during unmounting
- The recommendation to create subdirectories provides isolation and better control over the database environment
- The function only handles specific mount point indicators (errors 2 and 3); other error conditions are handled elsewhere
- The warnings help prevent subtle configuration issues that might not be immediately apparent but could cause problems later
- This is a user education function that improves the overall robustness of PostgreSQL installations