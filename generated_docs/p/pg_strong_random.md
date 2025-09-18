# pg_strong_random

## Location
src/port/pg_strong_random.c: 153 - 182

## Overview
Generates cryptographically secure random bytes suitable for authentication, key generation, and other security-sensitive operations.

## Definition
```c
bool pg_strong_random(void *buf, size_t len)
```

## Detailed Description
The pg_strong_random function fills a buffer with cryptographically secure random bytes. The implementation uses different system facilities depending on build configuration:

1. **OpenSSL builds**: Uses OpenSSL's RAND_bytes() function with automatic seeding via RAND_poll() retries to ensure the CSPRNG (Cryptographically Secure Pseudo-Random Number Generator) is sufficiently seeded.

2. **Windows builds**: Uses Windows CryptGenRandom API with lazy initialization of the crypto provider (HCRYPTPROV).

3. **Other platforms**: Directly reads from /dev/urandom using standard POSIX file operations, handling partial reads and signal interruptions.

The function is designed to run early in startup and cannot rely on backend infrastructure. It provides security-grade randomness suitable for generating salts, query cancellation keys, UUIDs, and other cryptographic material.

## Parameters / Member Variables
- `buf`: Pointer to the buffer where random bytes will be written
- `len`: Number of random bytes to generate
- **Returns**: `true` on success, `false` if random data generation failed

## Dependencies
- Functions called/Symbols referenced:
  - ssize_t (type)
  - open (POSIX file operations)
  - read (POSIX file operations) 
  - close (POSIX file operations)
  - EINTR (errno constant)
- Called from (representative examples):
  - InitControlFile
  - pg_be_scram_build_secret
  - build_server_first_message
  - CheckMD5Auth
  - RandomCancelKey
  - gen_random_uuid
  - pg_prng_strong_seed
  - pg_backend_random

## Notes and Other Information
- Must call pg_strong_random_init() once per process before using this function
- Always check the return value - proceeding with false return leads to security vulnerabilities
- Handles partial reads and signal interruptions gracefully on Unix-like systems
- For OpenSSL builds, retries RAND_poll() up to 8 times if CSPRNG is insufficiently seeded
- Used throughout PostgreSQL for generating cryptographically secure random data
- Implementation varies by platform but provides consistent security guarantees