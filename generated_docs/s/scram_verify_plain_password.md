# scram_verify_plain_password

## Location
[src/backend/libpq/auth-scram.c:512-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L512-L588)

## Overview
Verifies a plaintext password against a stored SCRAM secret, enabling plaintext password authentication for users with SCRAM secrets.

## Definition

```c
bool
scram_verify_plain_password(const char *username, const char *password,
							const char *secret)
```
## Detailed Description
This function enables plaintext password authentication for users who have SCRAM secrets stored in pg_authid.rolpassword. It works by parsing the stored SCRAM secret to extract the salt, iteration count, and server key, then computing what the server key should be for the provided plaintext password using the same parameters. If the computed server key matches the stored one, the password is verified as correct.

The verification process involves:
1. Parsing the stored SCRAM secret to extract cryptographic parameters
2. Decoding the base64-encoded salt
3. Normalizing the plaintext password with SASLprep (if possible)  
4. Computing the salted password using PBKDF2
5. Deriving the server key from the salted password
6. Comparing the computed server key with the stored one

This function is critical for supporting mixed authentication modes where users have SCRAM secrets but need to authenticate via plaintext methods.

## Parameters / Member Variables
- : Username for error reporting purposes
- : Plaintext password to verify
- : Stored SCRAM secret from pg_authid.rolpassword

## Dependencies
- Functions called/Symbols referenced:
  - [parse_scram_secret](../p/parse_scram_secret.md)
  - pg_b64_dec_len
  - pg_b64_decode
  - [pg_saslprep](../p/pg_saslprep.md)
  - scram_SaltedPassword
  - scram_ServerKey
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - memcmp
  - strlen
  - ereport/errmsg
  - elog
  - SCRAM_MAX_KEY_LEN
  - SASLPREP_SUCCESS
- Called from (representative examples):
  - [plain_crypt_verify](../p/plain_crypt_verify.md) (src/backend/libpq/crypt.c:237)

## Notes and Other Information
- Returns true if password matches the SCRAM secret, false otherwise
- This is a public function exported via src/include/libpq/scram.h
- Handles SASLprep normalization gracefully, using original password if normalization fails
- Performs proper memory management for allocated salt and normalized password
- Uses constant-time comparison (memcmp) for the final key verification
- Logs detailed error messages for invalid SCRAM secrets
- Critical for enabling plaintext authentication methods (like LDAP, PAM) with SCRAM-stored passwords
- The function validates the SCRAM secret format before attempting cryptographic operations