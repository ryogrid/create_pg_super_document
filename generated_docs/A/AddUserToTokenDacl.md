# AddUserToTokenDacl

## Location
src/common/exec.c: 538 - 679

## Overview
Modifies the Discretionary Access Control List (DACL) of a Windows security token to explicitly include the current user account, addressing Windows security restrictions that affect restricted process creation.

## Definition


## Detailed Description
This Windows-specific function addresses security changes introduced in Windows XP/2003 patches and Vista/2008 that modify default DACL behavior. When PostgreSQL creates restricted processes by stripping Administrator privileges, the resulting token may only contain System permissions, leading to access denied errors for subsequent CreatePipe() and CreateProcess() calls. The function rebuilds the token's DACL by copying existing Access Control Entries (ACEs) and adding a new ACE that grants GENERIC_ALL access to the current user's SID, maintaining security while ensuring proper functionality.

## Parameters / Member Variables
- : Handle to the Windows security token whose DACL should be modified

## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves token DACL information
  -  - Allocates memory for Windows structures
  -  - Gets Windows error codes
  -  - PostgreSQL error logging function
  -  - Retrieves ACL metadata
  -  - Gets the current user's SID from token
  -  - Calculates SID size for memory allocation
  -  - Initializes new ACL structure
  -  - Retrieves individual ACEs from existing ACL
  -  - Adds ACEs to new ACL
  -  - Adds new access-allowed ACE for current user
  -  - Applies modified DACL to token
  -  - Frees allocated Windows memory
- Called from (representative examples):
  -  (src/bin/pg_ctl/pg_ctl.c:1831)
  -  (src/common/restricted_token.c:99)

## Notes and Other Information
- Windows-only function, not compiled on other platforms
- Returns TRUE on success, FALSE on failure
- Implements comprehensive error handling with detailed Windows error codes
- Uses goto-based cleanup pattern for proper resource management
- Essential for PostgreSQL service functionality on Windows when running with elevated privileges
- Addresses specific Windows security model changes that affect process creation
- Preserves security by only adding necessary permissions for the current user
- Memory allocation and cleanup handled through Windows LocalAlloc/LocalFree APIs