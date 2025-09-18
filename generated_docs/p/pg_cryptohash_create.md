# pg_cryptohash_create

## Location
src/common/cryptohash_openssl.c: 122 - 177

## Overview
Allocates and initializes a cryptographic hash context structure, with backend implementations for both generic and OpenSSL-specific environments.

## Definition
```c
pg_cryptohash_ctx *pg_cryptohash_create(pg_cryptohash_type type)
```

## Detailed Description
This function creates a new cryptographic hash context of the specified type (MD5, SHA1, SHA224, SHA256, SHA384, or SHA512). The implementation differs between the generic version (cryptohash.c) and the OpenSSL-specific version (cryptohash_openssl.c):

**Generic Implementation (cryptohash.c)**:
- Allocates memory for a `pg_cryptohash_ctx` structure (sized for the largest hash type)
- Initializes the context with the specified type and no error state
- Returns NULL on out-of-memory conditions

**OpenSSL Implementation (cryptohash_openssl.c)**:
- Pre-enlarges the resource owner to ensure space for tracking the context
- Allocates and initializes the context structure
- Creates an OpenSSL EVP_MD_CTX for the actual cryptographic operations
- Registers the context with PostgreSQL's resource management system (backend only)
- Handles memory allocation failures differently for backend vs frontend

The function ensures proper initialization and, in the OpenSSL version, integrates with PostgreSQL's resource management system for automatic cleanup.

## Parameters / Member Variables
- `type`: The type of cryptographic hash to create (pg_cryptohash_type enum value such as PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, or PG_SHA512)

## Dependencies
- Functions called/Symbols referenced:
  - ALLOC (memory allocation macro)
  - memset (standard library)
  - ResourceOwnerEnlarge (OpenSSL version, backend only)
  - ERR_clear_error (OpenSSL function)
  - EVP_MD_CTX_create (OpenSSL function)
  - ResourceOwnerRememberCryptoHash (OpenSSL version, backend only)
- Called from (representative examples):
  - InitializeBackupManifest
  - scram_mock_salt
  - cryptohash_internal
  - pg_checksum_init
  - pg_hmac_create
  - pg_md5_hash
  - scram_H

## Notes and Other Information
- Two implementations exist: generic (src/common/cryptohash.c:74-99) and OpenSSL-specific (src/common/cryptohash_openssl.c:122-169)
- The generic version always allocates space for the largest hash type to simplify memory management
- The OpenSSL version integrates with PostgreSQL's resource owner system for automatic cleanup in backend processes
- In the backend, memory allocation failures result in ereport(ERROR) rather than returning NULL
- The OpenSSL version clears any previous OpenSSL errors before creating the new context
- Context must be initialized with pg_cryptohash_init() before use
- Contexts should be freed with pg_cryptohash_free() when no longer needed