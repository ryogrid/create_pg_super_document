# CheckPasswordAuth

## Location
[src/backend/libpq/auth.c:795-829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L795-L829)

## Overview
CheckPasswordAuth implements plaintext password authentication for PostgreSQL client connections, handling the exchange of password credentials and their verification against stored role passwords.

## Definition

```c
static int
CheckPasswordAuth(Port *port, const char **logdetail)
```
## Detailed Description
CheckPasswordAuth is a core authentication function that implements the plaintext password authentication mechanism in PostgreSQL. It orchestrates the complete password authentication flow by first sending an authentication request to the client, receiving the password response, retrieving the stored password hash for the user, and verifying the provided password against the stored credentials. The function handles memory management for sensitive password data and sets the authenticated identity upon successful verification.

## Parameters / Member Variables
- `*port`: Connection port structure containing client connection information and user details
- `**logdetail`: Output parameter for detailed error messages that can be included in authentication logs
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

## Simplified Source

```c
static int
CheckPasswordAuth(Port *port, const char **logdetail)
{
    char *passwd;
    int result;
    char *shadow_pass;

    // Request plaintext password from client
    sendAuthRequest(port, AUTH_REQ_PASSWORD, NULL, 0);

    // Receive password from client
    passwd = recv_password_packet(port);
    if (passwd == NULL)
        return STATUS_EOF;

    // Get stored password hash for user
    shadow_pass = get_role_password(port->user_name, logdetail);

    // Verify password if we have stored hash
    if (shadow_pass) {
        result = plain_crypt_verify(port->user_name, shadow_pass, passwd, logdetail);
    } else {
        result = STATUS_ERROR;
    }

    // Clean up sensitive memory
    if (shadow_pass)
        pfree(shadow_pass);
    pfree(passwd);

    // Set authenticated identity on success
    if (result == STATUS_OK)
        set_authn_id(port, port->user_name);

    return result;
}
```