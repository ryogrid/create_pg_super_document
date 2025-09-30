# scram_SaltedPassword

## Location
[src/common/scram-common.c:38-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/scram-common.c#L38-L111)

## Overview
Calculates the SaltedPassword component used in SCRAM authentication by implementing PBKDF2 with HMAC as the pseudorandom function.

## Definition
```c
int scram_SaltedPassword(const char *password,
                        pg_cryptohash_type hash_type, int key_length,
                        const char *salt, int saltlen, int iterations,
                        uint8 *result, const char **errstr)
```

## Detailed Description
This function implements the PBKDF2 (Password-Based Key Derivation Function 2) algorithm as specified in RFC2898, using HMAC as the pseudorandom function. It derives a cryptographic key from a password by applying a hash function repeatedly (iterations) with a salt. The password should already be normalized by SASLprep before calling this function. The function performs iterative HMAC calculations where each iteration uses the result of the previous iteration, and all results are XORed together to produce the final salted password.

## Parameters / Member Variables
- `password`: The input password string (should be SASLprep normalized)
- `hash_type`: The cryptographic hash type to use (e.g., SHA-256)
- `key_length`: The desired length of the derived key in bytes
- `salt`: The salt bytes to use in the derivation
- `saltlen`: The length of the salt in bytes  
- `iterations`: The number of PBKDF2 iterations to perform
- `result`: Output buffer to store the derived salted password
- `errstr`: Pointer to error message string on failure

## Dependencies
- Functions called/Symbols referenced:
  - [pg_hmac_create](../p/pg_hmac_create.md)
  - [pg_hmac_init](../p/pg_hmac_init.md)
  - [pg_hmac_update](../p/pg_hmac_update.md)
  - [pg_hmac_final](../p/pg_hmac_final.md)
  - [pg_hmac_error](../p/pg_hmac_error.md)
  - [pg_hmac_free](../p/pg_hmac_free.md)
  - pg_hton32
  - CHECK_FOR_INTERRUPTS (backend only)
- Called from (representative examples):
  - [scram_verify_plain_password](scram_verify_plain_password.md)
  - [scram_build_secret](scram_build_secret.md)
  - [calculate_client_proof](../c/calculate_client_proof.md)

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses interruptible processing in backend builds to handle large iteration counts
- The function implements the core PBKDF2 algorithm where: SaltedPassword = PBKDF2(password, salt, iterations)
- Memory management is handled automatically through pg_hmac_free() cleanup
- Thread-safe as it uses local variables and doesn't modify global state

## Simplified Source

```c
int scram_SaltedPassword(const char *password,
                        pg_cryptohash_type hash_type, int key_length,
                        const char *salt, int saltlen, int iterations,
                        uint8 *result, const char **errstr)
{
    int password_len = strlen(password);
    uint32 one = pg_hton32(1);
    uint8 Ui[SCRAM_MAX_KEY_LEN];
    uint8 Ui_prev[SCRAM_MAX_KEY_LEN];

    // Create HMAC context
    pg_hmac_ctx *hmac_ctx = pg_hmac_create(hash_type);
    if (hmac_ctx == NULL) {
        *errstr = pg_hmac_error(NULL);
        return -1;
    }

    // First iteration: HMAC(password, salt || 1)
    if (pg_hmac_init(hmac_ctx, (uint8 *) password, password_len) < 0 ||
        pg_hmac_update(hmac_ctx, (uint8 *) salt, saltlen) < 0 ||
        pg_hmac_update(hmac_ctx, (uint8 *) &one, sizeof(uint32)) < 0 ||
        pg_hmac_final(hmac_ctx, Ui_prev, key_length) < 0) {
        *errstr = pg_hmac_error(hmac_ctx);
        pg_hmac_free(hmac_ctx);
        return -1;
    }

    memcpy(result, Ui_prev, key_length);

    // Subsequent iterations: XOR all results together
    for (int i = 1; i < iterations; i++) {
        // HMAC(password, Ui_prev)
        if (pg_hmac_init(hmac_ctx, (uint8 *) password, password_len) < 0 ||
            pg_hmac_update(hmac_ctx, (uint8 *) Ui_prev, key_length) < 0 ||
            pg_hmac_final(hmac_ctx, Ui, key_length) < 0) {
            *errstr = pg_hmac_error(hmac_ctx);
            pg_hmac_free(hmac_ctx);
            return -1;
        }

        // XOR with accumulated result
        for (int j = 0; j < key_length; j++)
            result[j] ^= Ui[j];
        memcpy(Ui_prev, Ui, key_length);
    }

    pg_hmac_free(hmac_ctx);
    return 0;
}
```