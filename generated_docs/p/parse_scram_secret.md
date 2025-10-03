# parse_scram_secret

## Location
[src/backend/libpq/auth-scram.c:589-682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L589-L682)

## Overview
Parses and validates the format of a given SCRAM secret string, extracting authentication parameters including iteration count, salt, stored key, and server key.

## Definition

```c
bool
parse_scram_secret(const char *secret, int *iterations,
				   pg_cryptohash_type *hash_type, int *key_length,
				   char **salt, uint8 *stored_key, uint8 *server_key)
```
## Detailed Description
This function parses a SCRAM (Salted Challenge Response Authentication Mechanism) secret string formatted as:


The function validates each component of the secret:
- Verifies the scheme is "SCRAM-SHA-256"
- Extracts and validates the iteration count as a positive integer
- Decodes and validates the base64-encoded salt
- Decodes the base64-encoded stored key and server key, ensuring they match the expected key length

On successful parsing, all extracted values are returned via output parameters. The salt is returned as a base64-encoded null-terminated string that is palloc'd by this function.

## Parameters / Member Variables
- `*secret`: Input SCRAM secret string to be parsed
- `*iterations`: Output parameter for the iteration count extracted from the secret
- `*hash_type`: Output parameter set to the hash algorithm type (PG_SHA256)
- `*key_length`: Output parameter set to the key length (SCRAM_SHA_256_KEY_LEN)
- `**salt`: Output parameter for the base64-encoded salt string (palloc'd)
- `*stored_key`: Pre-allocated buffer (SCRAM_MAX_KEY_LEN) to receive the decoded stored key
- `*server_key`: Pre-allocated buffer (SCRAM_MAX_KEY_LEN) to receive the decoded server key
## Dependencies
- Functions called/Symbols referenced:
  - [pstrdup](pstrdup.md)
  - strtok
  - strcmp
  - strtol
  - [pg_b64_dec_len](pg_b64_dec_len.md)
  - [pg_b64_decode](pg_b64_decode.md)
  - [palloc](palloc.md)
  - memcpy
  - PG_SHA256
  - SCRAM_SHA_256_KEY_LEN
- Called from (representative examples):
  - [scram_init](../s/scram_init.md)
  - [scram_verify_plain_password](../s/scram_verify_plain_password.md)
  - [get_password_type](../g/get_password_type.md)

## Notes and Other Information
- Returns true on successful parsing, false on any validation failure
- Uses goto-based error handling with a single  label
- The caller is responsible for pre-allocating buffers for stored_key and server_key
- The salt output parameter is dynamically allocated and must be freed by the caller
- Currently only supports SCRAM-SHA-256 algorithm
- Part of PostgreSQL's SCRAM authentication implementation

## Simplified Source

```c
bool parse_scram_secret(const char *secret, int *iterations,
                        pg_cryptohash_type *hash_type, int *key_length,
                        char **salt, uint8 *stored_key, uint8 *server_key) {
    // Parse format: SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
    char *v = pstrdup(secret);
    char *scheme_str = strtok(v, "$");
    char *iterations_str = strtok(NULL, ":");
    char *salt_str = strtok(NULL, "$");
    char *storedkey_str = strtok(NULL, ":");
    char *serverkey_str = strtok(NULL, "");

    // Validate all components are present
    if (!scheme_str || !iterations_str || !salt_str || !storedkey_str || !serverkey_str)
        goto invalid_secret;

    // Validate scheme is SCRAM-SHA-256
    if (strcmp(scheme_str, "SCRAM-SHA-256") != 0)
        goto invalid_secret;
    *hash_type = PG_SHA256;
    *key_length = SCRAM_SHA_256_KEY_LEN;

    // Parse iteration count
    char *p;
    errno = 0;
    *iterations = strtol(iterations_str, &p, 10);
    if (*p || errno != 0)
        goto invalid_secret;

    // Validate and decode salt (return encoded version)
    int decoded_len = pg_b64_dec_len(strlen(salt_str));
    char *decoded_salt_buf = palloc(decoded_len);
    if (pg_b64_decode(salt_str, strlen(salt_str), decoded_salt_buf, decoded_len) < 0)
        goto invalid_secret;
    *salt = pstrdup(salt_str);

    // Decode stored key and server key
    decoded_len = pg_b64_dec_len(strlen(storedkey_str));
    char *decoded_stored_buf = palloc(decoded_len);
    if (pg_b64_decode(storedkey_str, strlen(storedkey_str), decoded_stored_buf, decoded_len) != *key_length)
        goto invalid_secret;
    memcpy(stored_key, decoded_stored_buf, *key_length);

    decoded_len = pg_b64_dec_len(strlen(serverkey_str));
    char *decoded_server_buf = palloc(decoded_len);
    if (pg_b64_decode(serverkey_str, strlen(serverkey_str), decoded_server_buf, decoded_len) != *key_length)
        goto invalid_secret;
    memcpy(server_key, decoded_server_buf, *key_length);

    return true;

invalid_secret:
    *salt = NULL;
    return false;
}
```