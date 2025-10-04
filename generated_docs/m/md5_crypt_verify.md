# md5_crypt_verify

## Location
[src/backend/libpq/crypt.c:156-209](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/crypt.c#L156-L209)

## Overview
Check MD5 authentication response, and return STATUS_OK or STATUS_ERROR.

## Definition
```c
int md5_crypt_verify(const char *role, const char *shadow_pass,
                     const char *client_pass,
                     const char *md5_salt, int md5_salt_len,
                     const char **logdetail)
```

## Detailed Description
This function verifies MD5 authentication responses from clients by comparing the client's response against the expected encrypted password. The function takes the user's stored password hash, the client's response, and the MD5 salt, then computes what the correct response should be and compares it with the client's actual response. On authentication failure, it provides detailed error messages for logging purposes.

## Parameters / Member Variables
- `role`: The username being authenticated
- `shadow_pass`: The user's correct password or password hash, as stored in pg_authid.rolpassword
- `client_pass`: The response given by the remote user to the MD5 challenge
- `md5_salt`: The salt used in the MD5 authentication challenge
- `md5_salt_len`: Length of the MD5 salt
- `logdetail`: Output parameter for error messages to be logged

## Dependencies
- Functions called/Symbols referenced:
  - [get_password_type](../g/get_password_type.md) (password type checking)
  - [pg_md5_encrypt](../p/pg_md5_encrypt.md) (MD5 encryption)
  - [psprintf](../p/psprintf.md) (formatted string creation)
- Called from:
  - [CheckMD5Auth](../C/CheckMD5Auth.md) (MD5 authentication handler)

## Notes and Other Information
- Returns STATUS_OK if authentication succeeds, STATUS_ERROR if it fails
- Requires that the stored password be in MD5 format
- Provides detailed error logging for authentication failures
- Part of PostgreSQL's authentication subsystem
- Handles both password format validation and actual credential verification

## Simplified Source

```c
int md5_crypt_verify(const char *role, const char *shadow_pass,
                     const char *client_pass,
                     const char *md5_salt, int md5_salt_len,
                     const char **logdetail) {
    char crypt_pwd[MD5_PASSWD_LEN + 1];
    const char *errstr = NULL;

    // Verify password is in MD5 format
    if (get_password_type(shadow_pass) != PASSWORD_TYPE_MD5) {
        *logdetail = psprintf(_("User \"%s\" has a password that cannot be used with MD5 authentication."), role);
        return STATUS_ERROR;
    }

    // Compute expected MD5 response using stored password + salt
    if (!pg_md5_encrypt(shadow_pass + strlen("md5"), md5_salt, md5_salt_len, crypt_pwd, &errstr)) {
        *logdetail = errstr;
        return STATUS_ERROR;
    }

    // Compare client response with expected response
    if (strcmp(client_pass, crypt_pwd) == 0) {
        return STATUS_OK;
    } else {
        *logdetail = psprintf(_("Password does not match for user \"%s\"."), role);
        return STATUS_ERROR;
    }
}
```