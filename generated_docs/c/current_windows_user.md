# current_windows_user

## Location
[src/test/regress/pg_regress.c:949-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L949-L998)

## Overview
Retrieves the account name and domain/realm name for the currently logged-in Windows user using Windows Security APIs.

## Definition
```c
static void current_windows_user(const char **acct, const char **dom)
```

## Detailed Description
The `current_windows_user` function obtains the current Windows user's account and domain information by using the Windows Security API. It opens a handle to the current process token, retrieves the token user information containing the user's Security Identifier (SID), then uses that SID to look up the corresponding account name and domain name. The function is based on the implementation used in PostgreSQL's SSPI authentication system (pg_SSPI_recvauth) and stores the results in static buffers for return to the caller.

This function is primarily used for SSPI (Security Support Provider Interface) authentication setup during PostgreSQL regression testing on Windows systems, allowing the test framework to determine the current user's credentials for authentication configuration.

## Parameters / Member Variables
- `acct`: Output parameter that receives a pointer to the account name string
- `dom`: Output parameter that receives a pointer to the domain/realm name string

## Dependencies
- Functions called/Symbols referenced:
  - bail (for error handling)
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
- Called from (representative examples):
  - [config_sspi_auth](config_sspi_auth.md)

## Notes and Other Information
- Windows-specific function that uses Win32 API calls (OpenProcessToken, GetTokenInformation, LookupAccountSid)
- Uses static buffers (MAXPGPATH size) to store the account and domain names
- The returned string pointers remain valid until the next call to the function
- Calls bail() on any Windows API errors, terminating the program with appropriate error messages
- Part of the PostgreSQL regression testing framework's Windows SSPI authentication support
- Based on the pg_SSPI_recvauth() implementation from PostgreSQL's main authentication system
- Only compiled and used on Windows platforms

## Simplified Source

```c
static void current_windows_user(const char **acct, const char **dom) {
    static char accountname[MAXPGPATH];
    static char domainname[MAXPGPATH];
    HANDLE token;
    TOKEN_USER *tokenuser;
    DWORD retlen;
    DWORD accountnamesize = sizeof(accountname);
    DWORD domainnamesize = sizeof(domainname);
    SID_NAME_USE accountnameuse;

    // Open current process token for reading
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_READ, &token)) {
        bail("could not open process token: error code %lu", GetLastError());
    }

    // Get required buffer size for token information
    if (!GetTokenInformation(token, TokenUser, NULL, 0, &retlen) &&
        GetLastError() != 122) {
        bail("could not get token information buffer size: error code %lu",
             GetLastError());
    }

    // Allocate and retrieve token user information
    tokenuser = pg_malloc(retlen);
    if (!GetTokenInformation(token, TokenUser, tokenuser, retlen, &retlen)) {
        bail("could not get token information: error code %lu", GetLastError());
    }

    // Look up account and domain names from SID
    if (!LookupAccountSid(NULL, tokenuser->User.Sid, accountname, &accountnamesize,
                          domainname, &domainnamesize, &accountnameuse)) {
        bail("could not look up account SID: error code %lu", GetLastError());
    }

    free(tokenuser);

    // Return pointers to static buffers
    *acct = accountname;
    *dom = domainname;
}
```