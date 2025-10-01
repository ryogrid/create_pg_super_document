# pg_be_scram_build_secret

## Location
[src/backend/libpq/auth-scram.c:472-511](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L472-L511)

## Overview
Constructs a SCRAM-SHA-256 secret from a plain text password for storage in pg_authid.rolpassword.

## Definition

```c
char *
pg_be_scram_build_secret(const char *password)
```
## Detailed Description
This function creates a SCRAM-SHA-256 authentication secret from a plain text password that can be stored in the PostgreSQL system catalog. The process involves normalizing the password using SASLprep (if possible), generating a cryptographically secure random salt, and computing the SCRAM secret using the configured iteration count. The function handles password normalization failures gracefully by using the original password if SASLprep normalization is not possible (due to invalid UTF-8 or prohibited characters).

The resulting secret contains the salt, iteration count, and cryptographic keys needed for SCRAM authentication, formatted as a string suitable for storage in pg_authid.rolpassword.

## Parameters / Member Variables
- : Plain text password to be converted into a SCRAM secret

## Dependencies
- Functions called/Symbols referenced:
  - [pg_saslprep](pg_saslprep.md)
  - [pg_strong_random](pg_strong_random.md)
  - [scram_build_secret](../s/scram_build_secret.md)
  - [pfree](pfree.md)
  - ereport/errcode/errmsg
  - PG_SHA256
  - SCRAM_SHA_256_KEY_LEN
  - SCRAM_DEFAULT_SALT_LEN
  - SASLPREP_SUCCESS
  - scram_sha_256_iterations (global variable)
- Called from (representative examples):
  - [encrypt_password](../e/encrypt_password.md) (src/backend/libpq/crypt.c:142)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- This is a public function (not static) exported via src/include/libpq/scram.h
- Uses SASLprep normalization when possible, but gracefully handles failures
- Generates cryptographically secure random salt using pg_strong_random()
- Uses the global scram_sha_256_iterations variable for iteration count
- The function is primarily used when creating new user passwords or changing existing ones
- Handles memory management carefully by freeing the normalized password if allocated
- Error handling includes reporting internal errors if random salt generation fails

## Simplified Source

```c
char *pg_be_scram_build_secret(const char *password) {
    char *prep_password;
    pg_saslprep_rc rc;
    char saltbuf[SCRAM_DEFAULT_SALT_LEN];
    char *result;
    const char *errstr = NULL;

    // Normalize password with SASLprep if possible
    rc = pg_saslprep(password, &prep_password);
    if (rc == SASLPREP_SUCCESS)
        password = (const char *) prep_password;

    // Generate cryptographically secure random salt
    if (!pg_strong_random(saltbuf, SCRAM_DEFAULT_SALT_LEN))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("could not generate random salt")));

    // Build SCRAM secret with salt and iteration count
    result = scram_build_secret(PG_SHA256, SCRAM_SHA_256_KEY_LEN,
                                saltbuf, SCRAM_DEFAULT_SALT_LEN,
                                scram_sha_256_iterations, password,
                                &errstr);

    // Clean up normalized password if allocated
    if (prep_password)
        pfree(prep_password);

    return result;
}
```