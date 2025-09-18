# pg_GSS_checkauth

## Location
src/backend/libpq/auth.c: 1081 - 1187

## Overview
pg_GSS_checkauth validates that a GSSAPI-authenticated user is authorized to connect as the requested database user, performing principal name extraction, realm verification, and user mapping.

## Definition


## Detailed Description
pg_GSS_checkauth performs the final authorization phase of GSSAPI authentication after the security context has been successfully established. It extracts the authenticated principal name from the GSS context, validates the Kerberos realm against configured requirements, handles realm inclusion/exclusion based on HBA configuration, and performs user mapping to determine if the authenticated principal is authorized to connect as the requested PostgreSQL user. The function sets the authenticated identity and stores the original principal name for logging and display purposes.

## Parameters / Member Variables
- : Connection port structure containing client connection information, GSS authentication state, and HBA configuration

## Dependencies
- Functions called/Symbols referenced:
  - gss_display_name (extracts principal name from GSS context)
  - gss_release_buffer (releases GSS buffer resources)
  - palloc (allocates memory for principal name)
  - MemoryContextStrdup (stores principal in TopMemoryContext)
  - set_authn_id (sets authenticated identity for the connection)
  - strchr (locates realm separator '@' in principal name)
  - pg_strcasecmp/strcmp (case-sensitive/insensitive realm comparison)
  - check_usermap (validates user mapping authorization)
  - pg_GSS_error (error reporting for GSS failures)
  - pfree (memory cleanup)
- Called from (representative examples):
  - pg_GSS_recvauth function in auth.c:1073
  - ClientAuthentication function in auth.c:566

## Notes and Other Information
- Handles both case-sensitive and case-insensitive realm matching based on pg_krb_caseins_users
- Supports optional realm inclusion in username via include_realm HBA option
- Validates GSS principal realm against configured krb_realm in HBA
- Returns STATUS_ERROR for realm mismatches or missing realms when required
- Sets authenticated identity immediately upon successful GSS name extraction
- Stores original principal name in backend memory for later display
- Delegates final authorization to check_usermap function
- Properly handles null-termination of GSS buffer content
- Part of the two-phase GSSAPI authentication: context establishment (pg_GSS_recvauth) and authorization (pg_GSS_checkauth)