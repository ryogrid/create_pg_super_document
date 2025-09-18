# CheckTablespaceDirectory

## Location
src/backend/access/transam/xlogrecovery.c: 2143 - 2174

## Overview
CheckTablespaceDirectory verifies that the pg_tblspc directory contains only symbolic links and no real directories during recovery consistency checks.

## Definition


## Detailed Description
CheckTablespaceDirectory performs a crucial validation during WAL recovery to ensure that the pg_tblspc directory structure is correct. This function addresses a specific issue where replay of database creation XLOG records for databases that were later dropped can create fake directories in pg_tblspc.

The function performs the following operations:

1. **Directory Traversal**: Opens and reads through all entries in the pg_tblspc directory
2. **Entry Filtering**: Examines only entries that have numeric names (OID-based naming)
3. **Type Verification**: Checks that each numeric entry is a symbolic link, not a real directory
4. **Error Reporting**: Issues either a WARNING or PANIC (based on allow_in_place_tablespaces setting) if real directories are found

The validation is essential because PostgreSQL expects all tablespace references in pg_tblspc to be symbolic links to actual tablespace locations. Real directories in this location indicate inconsistent state that should have been cleaned up during recovery.

## Parameters / Member Variables
This function takes no parameters and operates on the global pg_tblspc directory.

## Dependencies
- Functions called/Symbols referenced:
  - AllocateDir
  - ReadDir  
  - get_dirent_type
  - snprintf
  - strspn
  - strlen
  - ereport
- Called from:
  - CheckRecoveryConsistency (src/backend/access/transam/xlogrecovery.c:2243)

## Notes and Other Information
- This is a static function only called from within the xlogrecovery.c module
- The function is called at the point where consistent state is reached during recovery
- The allow_in_place_tablespaces GUC parameter controls whether violations cause PANIC or just WARNING
- Only directory entries with purely numeric names (OIDs) are checked
- The function helps detect and prevent corruption from incomplete recovery operations
- Real directories in pg_tblspc typically indicate failed cleanup of dropped databases/tablespaces
- The function provides detailed error messages with hints for resolution when violations are found
- This check is part of ensuring data consistency before allowing normal database operations