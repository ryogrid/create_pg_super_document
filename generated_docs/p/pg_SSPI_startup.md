# pg_SSPI_startup

## Location
[src/interfaces/libpq/fe-auth.c:351-421](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L351-L421)

## Overview
Initiates SSPI authentication by acquiring credentials, setting up the target principal, and delegating to the continuation function for the first authentication exchange.

## Definition


## Detailed Description
This function handles the initialization phase of SSPI (Security Support Provider Interface) authentication for PostgreSQL client connections on Windows systems. It performs comprehensive setup for Windows integrated authentication using either Kerberos or NTLM protocols. The function:

1. **Duplicate Prevention**: Prevents multiple SSPI authentication attempts on the same connection
2. **Credential Acquisition**: Uses  to obtain authentication credentials for the specified security package
3. **Principal Construction**: Builds the target service principal name in SSPI format (service/hostname)
4. **Package Selection**: Chooses between 'kerberos' (Unix-compatible) or 'negotiate' (supports both Kerberos and NTLM) packages
5. **State Setup**: Sets connection flags to indicate SSPI authentication mode
6. **Delegation**: Calls  to begin the actual token exchange

The function provides flexibility in authentication protocols while maintaining compatibility with Unix-based PostgreSQL servers when using Kerberos.

## Parameters / Member Variables
- : PostgreSQL connection structure for storing SSPI context, credentials, and connection state
- : Flag to select security package (0 = kerberos only, 1 = negotiate package supporting Kerberos/NTLM)
- : Length of any incoming authentication data (typically 0 for initial startup)

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation for credentials handle and target principal string
  -  - Windows SSPI function to obtain authentication credentials
  -  - String formatting to construct service principal name
  -  - String length calculation for memory allocation
  -  - Error reporting for SSPI failures
  -  - Handles the actual authentication token exchange
  -  - Connection error reporting
- Called from (representative examples):
  -  - Main authentication dispatcher when SSPI method is selected

## Notes and Other Information
- This is a static function internal to the libpq authentication module on Windows
- Requires SSPI/Windows authentication support to be compiled and available
- Target principal format is 'service/hostname' (simpler than GSSAPI, no @REALM required)
- Uses  (typically 'postgres') as the service name component
- Sets  flag to ensure proper authentication flow continuation
- Supports outbound credentials only ()
- Provides Unix compatibility when  (Kerberos-only mode)
- Memory allocated for credentials and target principal is cleaned up by 
- Returns STATUS_OK on successful setup, STATUS_ERROR on validation or setup failures
- The actual SSPI token exchange begins immediately via delegation to 