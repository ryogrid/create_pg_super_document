# CheckPasswordAuth

## Location
[src/backend/libpq/auth.c:795-829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L795-L829)

## Overview
CheckPasswordAuth implements plaintext password authentication for PostgreSQL client connections, handling the exchange of password credentials and their verification against stored role passwords.

## Definition


## Detailed Description
CheckPasswordAuth is a core authentication function that implements the plaintext password authentication mechanism in PostgreSQL. It orchestrates the complete password authentication flow by first sending an authentication request to the client, receiving the password response, retrieving the stored password hash for the user, and verifying the provided password against the stored credentials. The function handles memory management for sensitive password data and sets the authenticated identity upon successful verification.

## Parameters / Member Variables
- : Connection port structure containing client connection information and user details
- : Output parameter for detailed error messages that can be included in authentication logs

## Dependencies
- Functions called/Symbols referenced:
  - [sendAuthRequest](../s/sendAuthRequest.md) (sends AUTH_REQ_PASSWORD request to client)
  - [recv_password_packet](../r/recv_password_packet.md) (receives password from client)
  - [get_role_password](../g/get_role_password.md) (retrieves stored password hash for user)
  - [plain_crypt_verify](../p/plain_crypt_verify.md) (verifies plaintext password against stored hash)
  - [set_authn_id](../s/set_authn_id.md) (sets authenticated identity on successful auth)
  - [pfree](../p/pfree.md) (memory cleanup)
- Called from (representative examples):
  - [ClientAuthentication](ClientAuthentication.md) function in auth.c:604

## Notes and Other Information
- Returns STATUS_OK on successful authentication, STATUS_EOF if client doesn't send password, or STATUS_ERROR on authentication failure
- Handles memory cleanup for both received password and retrieved shadow password
- Part of the password-based authentication mechanisms in PostgreSQL
- Uses secure password verification through plain_crypt_verify function
- Sets authentication identity only after successful password verification