# encrypt_password

## Location
[src/backend/libpq/crypt.c:107-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/crypt.c#L107-L154)

## Overview
Converts a user-supplied password into a cryptographically hashed secret of the specified target type (MD5 or SCRAM-SHA-256).

## Definition

```c
char *
encrypt_password(PasswordType target_type, const char *role,
                 const char *password)
```

## Detailed Description
This function handles password encryption for PostgreSQL's authentication system. It takes a plaintext password and converts it to the requested encrypted format. If the input password is already encrypted, it cannot be converted to another format and is returned as-is.

The function supports multiple password types:
- PASSWORD_TYPE_MD5: Uses MD5 hashing with salt
- PASSWORD_TYPE_SCRAM_SHA_256: Uses SCRAM-SHA-256 encryption
- PASSWORD_TYPE_PLAINTEXT: Not allowed for encryption (throws error)

The function first determines the current password type using get_password_type(), then either returns the existing encrypted password or encrypts the plaintext based on the target type.

## Parameters / Member Variables
- `target_type`: The desired password encryption type (MD5 or SCRAM-SHA-256)
- `role`: The role name to use as salt for MD5 encryption
- `password`: The password string to encrypt (can be plaintext or already encrypted)

## Dependencies
- Functions called/Symbols referenced:
  - [get_password_type](../g/get_password_type.md)
  - pstrdup
  - palloc
  - pg_md5_encrypt
  - pg_be_scram_build_secret
  - elog

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Cannot convert between encrypted password formats
- MD5 encryption includes the role name as salt
- SCRAM-SHA-256 uses PostgreSQL's built-in SCRAM implementation
- Located in src/backend/libpq/crypt.c:107-154

## Simplified Source

```c
char *
encrypt_password(PasswordType target_type, const char *role,
                 const char *password)
{
    PasswordType current_type = get_password_type(password);
    char       *encrypted_password;
    const char *errstr = NULL;

    // If already encrypted, return as-is (cannot convert between formats)
    if (current_type != PASSWORD_TYPE_PLAINTEXT)
        return pstrdup(password);

    // Encrypt based on target type
    switch (target_type)
    {
        case PASSWORD_TYPE_MD5:
            // Allocate space for MD5 hash
            encrypted_password = palloc(MD5_PASSWD_LEN + 1);

            // Generate MD5 hash with role name as salt
            if (!pg_md5_encrypt(password, role, strlen(role),
                                encrypted_password, &errstr))
                elog(ERROR, "password encryption failed: %s", errstr);
            return encrypted_password;

        case PASSWORD_TYPE_SCRAM_SHA_256:
            // Generate SCRAM-SHA-256 hash
            return pg_be_scram_build_secret(password);

        case PASSWORD_TYPE_PLAINTEXT:
            elog(ERROR, "cannot encrypt password with 'plaintext'");
    }

    // Should never reach here
    elog(ERROR, "cannot encrypt password to requested type");
    return NULL;
}
```