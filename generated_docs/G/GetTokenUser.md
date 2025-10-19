# GetTokenUser

## Location
[src/common/exec.c:680-731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L680-L731)

## Overview
A Windows-specific utility function that retrieves user token information from a process token, returning the TOKEN_USER structure containing the user SID and attributes.

## Definition
```c
static BOOL GetTokenUser(HANDLE hToken, PTOKEN_USER *ppTokenUser)
```

## Detailed Description
GetTokenUser is a Windows authentication utility function that extracts user information from a process token handle. It performs a two-stage operation: first querying the required buffer size for the token information, then allocating memory and retrieving the actual TOKEN_USER data.

The function uses the Windows API GetTokenInformation() with TokenUser information class to obtain the user Security Identifier (SID) and associated attributes from the provided token. This is essential for Windows security operations where the current user's identity needs to be determined from a token.

The function follows Windows API conventions for dynamic buffer allocation - it first calls GetTokenInformation with a NULL buffer to determine the required size, then allocates the appropriate amount of memory using LocalAlloc(), and finally retrieves the actual token data.

## Parameters / Member Variables
- `hToken`: Handle to an access token from which to retrieve user information
- `ppTokenUser`: Pointer to a PTOKEN_USER that will receive the allocated TOKEN_USER structure containing user SID and attributes

## Dependencies
- Functions called/Symbols referenced:
  - GetTokenInformation (Windows API)
  - GetLastError (Windows API)
  - LocalAlloc (Windows API) 
  - LocalFree (Windows API)
  - [log_error](../l/log_error.md) (PostgreSQL logging function)
- Called from (representative examples):
  - [AddUserToTokenDacl](../A/AddUserToTokenDacl.md)

## Notes and Other Information
- This is a Windows-specific function that is only compiled on Windows platforms
- The caller is responsible for calling LocalFree() on the returned TOKEN_USER memory to prevent memory leaks
- The function uses PostgreSQL's error logging system (log_error) to report failures
- Returns FALSE on any error condition (insufficient memory, API failures) and TRUE on success
- The function is static (internal to src/common/exec.c) and primarily used for Windows security token manipulation
- Part of PostgreSQL's Windows authentication and security infrastructure for handling restricted tokens and DACL modifications
- Memory allocation uses LPTR flag (LMEM_FIXED | LMEM_ZEROINIT) to get zero-initialized memory

## Simplified Source

```c
static BOOL GetTokenUser(HANDLE hToken, PTOKEN_USER *ppTokenUser) {
    DWORD buffer_size;

    *ppTokenUser = NULL;

    // First call: get required buffer size
    if (!GetTokenInformation(hToken, TokenUser, NULL, 0, &buffer_size)) {
        if (GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
            // Unexpected error - log and return failure
            return FALSE;
        }

        // Allocate memory for token user information
        *ppTokenUser = (PTOKEN_USER) LocalAlloc(LPTR, buffer_size);
        if (*ppTokenUser == NULL) {
            // Out of memory
            return FALSE;
        }
    }

    // Second call: get actual token user information
    if (!GetTokenInformation(hToken, TokenUser, *ppTokenUser, buffer_size, &buffer_size)) {
        // Failed to get token info - cleanup and return failure
        LocalFree(*ppTokenUser);
        *ppTokenUser = NULL;
        return FALSE;
    }

    // Success - caller must call LocalFree() on returned memory
    return TRUE;
}
```