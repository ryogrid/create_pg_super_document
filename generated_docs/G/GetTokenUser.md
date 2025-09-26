# GetTokenUser

## Location
src/common/exec.c: 680 - 731

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
  - log_error (PostgreSQL logging function)
- Called from (representative examples):
  - AddUserToTokenDacl

## Notes and Other Information
- This is a Windows-specific function that is only compiled on Windows platforms
- The caller is responsible for calling LocalFree() on the returned TOKEN_USER memory to prevent memory leaks
- The function uses PostgreSQL's error logging system (log_error) to report failures
- Returns FALSE on any error condition (insufficient memory, API failures) and TRUE on success
- The function is static (internal to src/common/exec.c) and primarily used for Windows security token manipulation
- Part of PostgreSQL's Windows authentication and security infrastructure for handling restricted tokens and DACL modifications
- Memory allocation uses LPTR flag (LMEM_FIXED | LMEM_ZEROINIT) to get zero-initialized memory