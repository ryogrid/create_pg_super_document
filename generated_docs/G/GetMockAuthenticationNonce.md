# GetMockAuthenticationNonce

## Location
src/backend/access/transam/xlog.c: 4533 - 4542

## Overview
Returns the random nonce from the PostgreSQL control file, used for mock authentication purposes during testing.

## Definition
```c
char *GetMockAuthenticationNonce(void)
```

## Detailed Description
GetMockAuthenticationNonce is an accessor function that retrieves the mock authentication nonce from the PostgreSQL control file. This nonce is a random value stored in the control file that is used during authentication testing scenarios. The function provides access to the mock_authentication_nonce field from the global ControlFile structure.

This function is primarily used in authentication-related code, particularly in SCRAM authentication testing where a predictable nonce value is needed for reproducible test results.

## Parameters / Member Variables
This function takes no parameters and returns a char pointer to the nonce string.

## Dependencies
- Functions called/Symbols referenced:
  - ControlFile (global variable access)
  - Assert (assertion check)
- Called from (representative examples):
  - [scram_mock_salt](../s/scram_mock_salt.md)
  - [WALAvailability](../W/WALAvailability.md) (header declaration)

## Notes and Other Information
- The function includes an assertion to ensure ControlFile is not NULL before accessing it
- The returned pointer points to memory owned by the ControlFile structure and should not be freed by the caller
- This function is primarily used in testing scenarios where reproducible authentication behavior is required
- The nonce value is stored persistently in the control file and remains constant across server restarts
- Located in src/backend/access/transam/xlog.c:4533-4542