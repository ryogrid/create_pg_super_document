# AddUserToTokenDacl

## Location
[src/common/exec.c:538-679](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L538-L679)

## Overview
Modifies the Discretionary Access Control List (DACL) of a Windows security token to explicitly include the current user account, addressing Windows security restrictions that affect restricted process creation.

## Definition

```c
BOOL
AddUserToTokenDacl(HANDLE hToken)
```
## Detailed Description
This Windows-specific function addresses security changes introduced in Windows XP/2003 patches and Vista/2008 that modify default DACL behavior. When PostgreSQL creates restricted processes by stripping Administrator privileges, the resulting token may only contain System permissions, leading to access denied errors for subsequent CreatePipe() and CreateProcess() calls. The function rebuilds the token's DACL by copying existing Access Control Entries (ACEs) and adding a new ACE that grants GENERIC_ALL access to the current user's SID, maintaining security while ensuring proper functionality.

## Parameters / Member Variables
- `hToken`: Handle to the Windows security token whose DACL should be modified
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

## Simplified Source

```c
BOOL AddUserToTokenDacl(HANDLE hToken) {
    ACL_SIZE_INFORMATION acl_info;
    ACCESS_ALLOWED_ACE *ace;
    DWORD new_acl_size, buffer_size = 0;
    PACL new_acl = NULL;
    PTOKEN_USER current_user = NULL;
    TOKEN_DEFAULT_DACL *token_dacl = NULL;
    TOKEN_DEFAULT_DACL new_token_dacl;
    BOOL success = FALSE;

    // Get token DACL information
    if (!GetTokenInformation(hToken, TokenDefaultDacl, NULL, 0, &buffer_size)) {
        if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) goto cleanup;

        token_dacl = (TOKEN_DEFAULT_DACL *) LocalAlloc(LPTR, buffer_size);
        if (!token_dacl || !GetTokenInformation(hToken, TokenDefaultDacl, token_dacl, buffer_size, &buffer_size)) {
            goto cleanup;
        }
    }

    // Get ACL size information
    if (!GetAclInformation(token_dacl->DefaultDacl, &acl_info, sizeof(acl_info), AclSizeInformation)) {
        goto cleanup;
    }

    // Get current user SID
    if (!GetTokenUser(hToken, &current_user)) {
        goto cleanup;
    }

    // Calculate new ACL size (existing + new ACE for current user)
    new_acl_size = acl_info.AclBytesInUse + sizeof(ACCESS_ALLOWED_ACE) +
                   GetLengthSid(current_user->User.Sid) - sizeof(DWORD);

    // Create and initialize new ACL
    new_acl = (PACL) LocalAlloc(LPTR, new_acl_size);
    if (!new_acl || !InitializeAcl(new_acl, new_acl_size, ACL_REVISION)) {
        goto cleanup;
    }

    // Copy existing ACEs to new ACL
    for (int i = 0; i < (int) acl_info.AceCount; i++) {
        if (!GetAce(token_dacl->DefaultDacl, i, (LPVOID *) &ace) ||
            !AddAce(new_acl, ACL_REVISION, MAXDWORD, ace, ((PACE_HEADER) ace)->AceSize)) {
            goto cleanup;
        }
    }

    // Add new ACE granting GENERIC_ALL to current user
    if (!AddAccessAllowedAceEx(new_acl, ACL_REVISION, OBJECT_INHERIT_ACE,
                               GENERIC_ALL, current_user->User.Sid)) {
        goto cleanup;
    }

    // Apply new DACL to token
    new_token_dacl.DefaultDacl = new_acl;
    if (!SetTokenInformation(hToken, TokenDefaultDacl, &new_token_dacl, new_acl_size)) {
        goto cleanup;
    }

    success = TRUE;

cleanup:
    // Free allocated memory
    if (current_user) LocalFree(current_user);
    if (new_acl) LocalFree(new_acl);
    if (token_dacl) LocalFree(token_dacl);

    return success;
}
```