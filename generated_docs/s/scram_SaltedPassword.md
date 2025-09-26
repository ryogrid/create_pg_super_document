# scram_SaltedPassword

## Location
src/common/scram-common.c: 38 - 111

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
  - pg_hmac_create
  - pg_hmac_init
  - pg_hmac_update
  - pg_hmac_final
  - pg_hmac_error
  - pg_hmac_free
  - pg_hton32
  - CHECK_FOR_INTERRUPTS (backend only)
- Called from (representative examples):
  - scram_verify_plain_password
  - scram_build_secret
  - calculate_client_proof

## Notes and Other Information
- Returns 0 on success, -1 on failure
- Uses interruptible processing in backend builds to handle large iteration counts
- The function implements the core PBKDF2 algorithm where: SaltedPassword = PBKDF2(password, salt, iterations)
- Memory management is handled automatically through pg_hmac_free() cleanup
- Thread-safe as it uses local variables and doesn't modify global state