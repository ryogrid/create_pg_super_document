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
- `*username`: Username for error reporting purposes
- `*password`: Plaintext password to verify
- `*secret`: Stored SCRAM secret from pg_authid.rolpassword
## Dependencies
- Functions called/Symbols referenced:
  - [parse_scram_secret](../p/parse_scram_secret.md)
  - [pg_b64_dec_len](../p/pg_b64_dec_len.md)
  - [pg_b64_decode](../p/pg_b64_decode.md)
  - [pg_saslprep](../p/pg_saslprep.md)
  - [scram_SaltedPassword](scram_SaltedPassword.md)
  - [scram_ServerKey](scram_ServerKey.md)
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

## Simplified Source

```c
bool scram_verify_plain_password(const char *username, const char *password,
                                 const char *secret) {
    char *encoded_salt;
    int iterations, key_length = 0;
    pg_cryptohash_type hash_type;
    uint8 stored_key[SCRAM_MAX_KEY_LEN];
    uint8 server_key[SCRAM_MAX_KEY_LEN];
    uint8 computed_key[SCRAM_MAX_KEY_LEN];

    // Parse the SCRAM secret to extract parameters
    if (!parse_scram_secret(secret, &iterations, &hash_type, &key_length,
                           &encoded_salt, stored_key, server_key)) {
        ereport(LOG, (errmsg("invalid SCRAM secret for user \"%s\"", username)));
        return false;
    }

    // Decode the base64 salt
    int saltlen = pg_b64_dec_len(strlen(encoded_salt));
    char *salt = palloc(saltlen);
    saltlen = pg_b64_decode(encoded_salt, strlen(encoded_salt), salt, saltlen);
    if (saltlen < 0) {
        ereport(LOG, (errmsg("invalid SCRAM secret for user \"%s\"", username)));
        return false;
    }

    // Normalize password with SASLprep
    char *prep_password = NULL;
    if (pg_saslprep(password, &prep_password) == SASLPREP_SUCCESS)
        password = prep_password;

    // Compute server key from plaintext password using same parameters
    uint8 salted_password[SCRAM_MAX_KEY_LEN];
    const char *errstr = NULL;
    if (scram_SaltedPassword(password, hash_type, key_length, salt, saltlen,
                            iterations, salted_password, &errstr) < 0 ||
        scram_ServerKey(salted_password, hash_type, key_length, computed_key, &errstr) < 0) {
        elog(ERROR, "could not compute server key: %s", errstr);
    }

    if (prep_password)
        pfree(prep_password);

    // Compare computed server key with stored one
    return memcmp(computed_key, server_key, key_length) == 0;
}
```