# pg_sha1_ctx

## Location
src/common/sha1_int.h: 74 - 81

## Overview
The  structure represents the context/state for SHA-1 hash computation in PostgreSQL, maintaining intermediate hash values, message counters, and buffer data throughout the hashing process.

## Definition

```c
union
	{
		uint8		b8[64];
		uint32		b32[16];
	}			m;
```
## Detailed Description
The  structure serves as the central state container for PostgreSQL's fallback SHA-1 implementation, based on FIPS pub 180-1. This structure is designed to efficiently store all the intermediate data required during SHA-1 computation, utilizing unions to provide both byte-level and word-level access to the same memory regions for optimal performance across different operations.

The structure is part of PostgreSQL's internal cryptographic hash implementation (src/common/sha1_int.h) and is used when external cryptographic libraries like OpenSSL are not available. It maintains the hash state across multiple calls to , allowing for incremental processing of large data streams.

## Parameters / Member Variables
- : Hash state union containing the 160-bit (20-byte) intermediate hash value
  - : Byte-level access to the hash state (20 bytes)
  - : Word-level access to the hash state (5 32-bit words)
- : Message length counter union for tracking total bytes processed
  - : Byte-level access to the 64-bit counter
  - : Direct 64-bit access to the counter
- : Message buffer union for storing partial blocks during processing
  - : Byte-level access to the 512-bit (64-byte) message buffer
  - : Word-level access to the message buffer (16 32-bit words)
- : Current number of bytes in the message buffer (0-63)

## Dependencies
- Functions called/Symbols referenced:
  - pg_sha1_init (initializes context)
  - pg_sha1_update (processes data)
  - pg_sha1_final (finalizes hash computation)
- Called from (representative examples):
  - pg_cryptohash_ctx (as part of unified hash context)
  - sha1_step (internal processing function)
  - sha1_pad (padding operation)
  - sha1_result (result extraction)

## Notes and Other Information
- This is PostgreSQL's fallback SHA-1 implementation, used when external crypto libraries are unavailable
- The structure uses unions to provide efficient access patterns for both byte-oriented and word-oriented operations
- Based on the WIDE Project's SHA-1 implementation and FIPS pub 180-1 specification
- The design allows for incremental hashing of data streams larger than memory
- Part of PostgreSQL's unified cryptographic hash framework alongside MD5 and SHA-2 variants
- Located in src/common/sha1_int.h as an internal implementation detail