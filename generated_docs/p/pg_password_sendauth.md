# pg_password_sendauth

## Location
[src/interfaces/libpq/fe-auth.c:700-766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L700-L766)

## Overview
Sends password-based authentication to the PostgreSQL server, handling both plaintext and MD5-encrypted password authentication methods.

## Definition

```c
static int
pg_password_sendauth(PGconn *conn, const char *password, AuthRequest areq)
```
## Detailed Description
The  function handles password-based authentication in PostgreSQL's libpq client library. It supports two authentication methods: plaintext password transmission (AUTH_REQ_PASSWORD) and MD5-encrypted password authentication (AUTH_REQ_MD5).

For MD5 authentication, the function performs double MD5 hashing: first hashing the password with the username, then hashing the result with a salt provided by the server. This approach protects against replay attacks while avoiding transmission of plaintext passwords. The function manages memory allocation for the encrypted passwords and ensures proper cleanup.

## Parameters / Member Variables
- : Pointer to the PGconn connection structure containing connection state and user information
- : The plaintext password to authenticate with
- : AuthRequest enum value specifying the authentication method (AUTH_REQ_PASSWORD or AUTH_REQ_MD5)

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetnchar](pqGetnchar.md)
  - malloc
  - [pg_md5_encrypt](pg_md5_encrypt.md)
  - [pqPacketSend](pqPacketSend.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - free
  - strlen
- Constants used:
  - AUTH_REQ_MD5
  - AUTH_REQ_PASSWORD
  - STATUS_ERROR
  - MD5_PASSWD_LEN
  - PqMsg_PasswordMessage
- Called from:
  - [pg_fe_sendauth](pg_fe_sendauth.md)

## Notes and Other Information
- For MD5 authentication, allocates space for two MD5 hashes (2 * (MD5_PASSWD_LEN + 1) bytes)
- The MD5 encryption process follows PostgreSQL's specific double-hashing scheme: md5(md5(password + username) + salt)
- Reads the 4-byte salt from the server for MD5 authentication before performing encryption
- Handles memory allocation failures gracefully with appropriate error messages
- The password message is NULL-terminated when sent to the server (strlen + 1)
- Returns STATUS_ERROR for unsupported authentication request types
- Properly cleans up allocated memory in all code paths, including error conditions