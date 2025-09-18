# CheckPWChallengeAuth

## Location
src/backend/libpq/auth.c: 830 - 889

## Overview
CheckPWChallengeAuth implements challenge-response authentication mechanisms for PostgreSQL, supporting both MD5 and SCRAM-SHA-256 authentication methods based on the user's stored password type.

## Definition


## Detailed Description
CheckPWChallengeAuth is a sophisticated authentication function that handles both MD5 and SCRAM-SHA-256 authentication mechanisms. It intelligently selects the appropriate authentication method based on the type of password hash stored for the user and the authentication method configured in pg_hba.conf. The function implements security measures to prevent user enumeration attacks by proceeding through authentication motions even when the user doesn't exist, using the current password_encryption setting to determine which authentication method to simulate.

## Parameters / Member Variables
- : Connection port structure containing client connection information, user details, and HBA (Host-Based Authentication) configuration
- : Output parameter for detailed error messages that can be included in authentication logs

## Dependencies
- Functions called/Symbols referenced:
  - [get_role_password](../g/get_role_password.md) (retrieves stored password hash for user)
  - [get_password_type](../g/get_password_type.md) (determines the type of stored password hash)
  - [CheckMD5Auth](CheckMD5Auth.md) (performs MD5 authentication when password is MD5 type)
  - [CheckSASLAuth](CheckSASLAuth.md) (performs SCRAM authentication with pg_be_scram_mech)
  - [set_authn_id](../s/set_authn_id.md) (sets authenticated identity on successful auth)
  - [pfree](../p/pfree.md) (memory cleanup)
- Called from (representative examples):
  - [ClientAuthentication](ClientAuthentication.md) function in auth.c:600

## Notes and Other Information
- Supports both uaMD5 and uaSCRAM authentication methods as configured in pg_hba.conf
- Implements anti-enumeration security by simulating authentication even for non-existent users
- Automatically chooses MD5 vs SCRAM based on stored password type when MD5 is allowed
- Always uses SCRAM when MD5 authentication is not permitted, even if user has MD5 password (causing authentication to fail)
- Returns STATUS_OK on successful authentication, error status otherwise
- Ensures authentication cannot succeed when get_role_password() fails
- Part of the password-based challenge-response authentication mechanisms in PostgreSQL