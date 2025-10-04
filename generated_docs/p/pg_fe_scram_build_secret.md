# pg_fe_scram_build_secret

## Location
[src/interfaces/libpq/fe-auth-scram.c:892-930](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth-scram.c#L892-L930)

## Overview
Builds a new SCRAM secret from a plain text password by performing SASLprep normalization, generating a random salt, and creating the SCRAM authentication secret.

## Definition

```c
char *
pg_fe_scram_build_secret(const char *password, int iterations, const char **errstr)
```
## Detailed Description
This function creates a SCRAM (Salted Challenge Response Authentication Mechanism) secret suitable for storage and use in PostgreSQL authentication. It performs the complete process of converting a plain text password into a SCRAM secret by first normalizing the password using SASLprep (RFC 4013), generating a cryptographically secure random salt, and then invoking the core SCRAM secret building functionality with SHA-256 hashing.

The function handles password normalization gracefully - if SASLprep normalization fails due to invalid UTF-8 or prohibited characters, it proceeds with the original password as recommended by the SCRAM specification. This ensures compatibility with a wide range of password formats while maintaining security best practices.

## Parameters / Member Variables
- `*password`: The plain text password to be converted into a SCRAM secret
- `iterations`: The number of PBKDF2 iterations to use for key derivation (higher values increase security but require more computation)
- `**errstr`: Output parameter that will point to an error message string if the function fails
## Dependencies
- Functions called/Symbols referenced:
  - [pg_saslprep](pg_saslprep.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - [pg_strong_random](pg_strong_random.md)
  - [scram_build_secret](../s/scram_build_secret.md)
  - free
- Constants used:
  - SCRAM_DEFAULT_SALT_LEN
  - PG_SHA256
  - SCRAM_SHA_256_KEY_LEN
  - SASLPREP_OOM
  - SASLPREP_SUCCESS
- Called from (representative examples):
  - Functions in fe-auth.c that handle password processing

## Notes and Other Information
- Returns a dynamically allocated string containing the SCRAM secret on success, NULL on failure
- The caller is responsible for freeing the returned string
- Uses SHA-256 as the hash algorithm (PG_SHA256)
- Generates a 16-byte random salt (SCRAM_DEFAULT_SALT_LEN)
- SASLprep normalization is applied but failures are handled gracefully
- Part of PostgreSQL's client-side authentication infrastructure in libpq
- The returned secret format is suitable for storage in PostgreSQL's system catalogs
- Error messages are internationalized using libpq_gettext

## Simplified Source

```c
char *pg_fe_scram_build_secret(const char *password, int iterations, const char **errstr) {
    char *prep_password;
    pg_saslprep_rc rc;
    char saltbuf[SCRAM_DEFAULT_SALT_LEN];
    char *result;

    // Normalize password with SASLprep (graceful failure handling)
    rc = pg_saslprep(password, &prep_password);
    if (rc == SASLPREP_OOM) {
        *errstr = libpq_gettext("out of memory");
        return NULL;
    }
    if (rc == SASLPREP_SUCCESS)
        password = (const char *) prep_password;

    // Generate cryptographically secure random salt
    if (!pg_strong_random(saltbuf, SCRAM_DEFAULT_SALT_LEN)) {
        *errstr = libpq_gettext("could not generate random salt");
        free(prep_password);
        return NULL;
    }

    // Build the actual SCRAM secret using SHA-256
    result = scram_build_secret(PG_SHA256, SCRAM_SHA_256_KEY_LEN, saltbuf,
                                SCRAM_DEFAULT_SALT_LEN, iterations, password, errstr);

    free(prep_password);
    return result;
}
```