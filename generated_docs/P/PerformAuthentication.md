# PerformAuthentication

## Location
src/backend/utils/init/postinit.c: 190 - 312

## Overview
PerformAuthentication is a static function that handles the complete authentication process for a remote client connection during PostgreSQL backend startup.

## Definition
static void PerformAuthentication(Port *port)

## Detailed Description
This function orchestrates the entire client authentication process during backend startup. It performs several critical tasks:

1. **Configuration Loading**: In EXEC_BACKEND builds, loads pg_hba.conf and pg_ident.conf files since they weren't inherited from the postmaster
2. **Timeout Management**: Sets up authentication timeout to prevent hanging on unresponsive or malicious clients
3. **Authentication Execution**: Calls the main authentication logic via ClientAuthentication()
4. **Connection Logging**: Logs successful connections with detailed information about SSL, GSS, and other connection properties
5. **Process State Updates**: Updates the process title and authentication flags

The function will not return if authentication fails - it will terminate the process. On successful authentication, it logs connection details including user, database, SSL information, and GSS authentication details if applicable.

## Parameters / Member Variables
- : Pointer to Port structure containing all client connection information and state

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (for creating PostmasterContext in EXEC_BACKEND)
  - [load_hba](../l/load_hba.md) (for loading pg_hba.conf configuration)
  - [load_ident](../l/load_ident.md) (for loading pg_ident.conf configuration) 
  - enable_timeout_after/disable_timeout (for authentication timeout management)
  - set_ps_display (for updating process title)
  - [ClientAuthentication](../C/ClientAuthentication.md) (main authentication logic)
  - initStringInfo/appendStringInfo (for building log messages)
  - [be_tls_get_version](../b/be_tls_get_version.md)/be_tls_get_cipher/be_tls_get_cipher_bits (SSL info)
  - [be_gssapi_get_princ](../b/be_gssapi_get_princ.md)/be_gssapi_get_auth/be_gssapi_get_enc/be_gssapi_get_delegation (GSS info)
  - ereport (for logging and error reporting)
- Called from:
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:926)

## Notes and Other Information
- This is a static function, only accessible within postinit.c
- Sets ClientAuthInProgress flag to limit log message visibility during authentication
- In EXEC_BACKEND builds, must create PostmasterContext and load configuration files
- Uses statement_timeout infrastructure for authentication timeout
- Does not return on authentication failure - process terminates
- Logs detailed connection information when Log_connections is enabled
- Supports SSL and GSS authentication methods with detailed logging
- Updates process title to show current authentication state
- Critical security function - handles all client authentication before database access