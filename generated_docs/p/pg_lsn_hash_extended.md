# pg_lsn_hash_extended

## Location
src/backend/utils/adt/pg_lsn.c: 213 - 223

## Overview
The pg_lsn_hash_extended function computes an extended hash value for a PostgreSQL Log Sequence Number (LSN) type, providing a hash function suitable for use in hash tables with seed values.

## Definition


## Detailed Description
This function serves as a wrapper around the hashint8extended function to provide extended hashing capabilities for the pg_lsn data type. Since LSN values are internally represented as 64-bit integers, this function leverages the existing 64-bit integer extended hash implementation. The extended hash variant accepts an additional seed parameter that can be used to randomize hash values, which is important for security and performance in hash-based data structures.

The function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS, making it suitable for direct invocation from SQL or internal PostgreSQL operations that require extended hashing of LSN values.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument mechanism:
- Function arguments are accessed through the fcinfo parameter structure
- The LSN value to hash is retrieved from the first argument
- An optional seed value for hash randomization is retrieved from the second argument

## Dependencies
- Functions called/Symbols referenced:
  - hashint8extended
- Called from (representative examples):
  - No direct callers found (typically invoked through PostgreSQL's function call mechanism)

## Notes and Other Information
- This function is part of PostgreSQL's LSN data type implementation
- LSN values are crucial for write-ahead logging and replication in PostgreSQL
- The extended hash variant is important for hash table implementations that require seed values for security or performance reasons
- The function directly delegates to hashint8extended since LSN values are internally stored as 64-bit integers
- Located in src/backend/utils/adt/pg_lsn.c:213-223