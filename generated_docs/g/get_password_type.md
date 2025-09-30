# get_password_type

## Location
[src/backend/libpq/crypt.c:84-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/crypt.c#L84-L105)

## Overview
Analyzes a password string to determine its encryption format (plaintext, MD5, or SCRAM-SHA-256).

## Definition

```c
PasswordType
get_password_type(const char *shadow_pass)
```

## Detailed Description
This function examines a password string to determine what type of encryption (if any) has been applied to it. It's used throughout PostgreSQL's authentication system to identify password formats for proper handling during authentication and password management operations.

The function performs pattern matching to identify:
1. MD5 passwords: Start with "md5" prefix and have specific length/character set
2. SCRAM-SHA-256 passwords: Can be parsed by parse_scram_secret()
3. Plaintext passwords: Everything else

The detection is performed in order of specificity to avoid false positives.

## Parameters / Member Variables
- `shadow_pass`: The password string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - strncmp
  - strlen
  - strspn
  - parse_scram_secret
  - MD5_PASSWD_LEN (constant)
  - MD5_PASSWD_CHARSET (constant)

## Notes and Other Information
- Returns PasswordType enum value indicating the detected format
- MD5 detection checks prefix, length, and character set
- SCRAM detection uses parse_scram_secret() which validates the full format
- Defaults to PASSWORD_TYPE_PLAINTEXT if no other format is detected
- Critical for authentication system to handle different password formats correctly
- Located in src/backend/libpq/crypt.c:84-105

## Simplified Source

```c
PasswordType
get_password_type(const char *shadow_pass)
{
    char       *encoded_salt;
    int         iterations;
    int         key_length = 0;
    pg_cryptohash_type hash_type;
    uint8       stored_key[SCRAM_MAX_KEY_LEN];
    uint8       server_key[SCRAM_MAX_KEY_LEN];

    // Check for MD5 format: "md5" + 32 hex characters
    if (strncmp(shadow_pass, "md5", 3) == 0 &&
        strlen(shadow_pass) == MD5_PASSWD_LEN &&
        strspn(shadow_pass + 3, MD5_PASSWD_CHARSET) == MD5_PASSWD_LEN - 3)
        return PASSWORD_TYPE_MD5;

    // Check for SCRAM-SHA-256 format
    if (parse_scram_secret(shadow_pass, &iterations, &hash_type, &key_length,
                           &encoded_salt, stored_key, server_key))
        return PASSWORD_TYPE_SCRAM_SHA_256;

    // Default to plaintext if no other format matches
    return PASSWORD_TYPE_PLAINTEXT;
}
```