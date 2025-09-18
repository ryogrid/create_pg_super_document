# DatumGetUInt64

## Location
src/include/postgres.h: 419 - 435

## Overview
DatumGetUInt64 extracts a 64-bit unsigned integer value from a Datum, providing platform-independent access to uint64 values regardless of whether they are stored by value or by reference.

## Definition


## Detailed Description
DatumGetUInt64 is a platform-aware function that extracts a 64-bit unsigned integer from a Datum representation. Like its signed counterpart DatumGetInt64, this function handles platform differences in 64-bit value storage automatically based on the USE_FLOAT8_BYVAL compilation flag.

On platforms where 64-bit values can be passed by value (typically 64-bit architectures), the function directly casts the Datum to uint64. On platforms where 64-bit values must be passed by reference (typically 32-bit architectures), it dereferences the pointer obtained via DatumGetPointer().

This function is commonly used in hash functions, partitioning operations, and other contexts where unsigned 64-bit arithmetic is required, providing a portable interface across different PostgreSQL installations.

## Parameters / Member Variables
- : A Datum containing a 64-bit unsigned integer value to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetPointer (on pass-by-reference platforms)
  - USE_FLOAT8_BYVAL (preprocessor flag)
- Called from (representative examples):
  - k_hashes (bloom filter implementation)
  - JumbleQuery (query fingerprinting)
  - compute_partition_hash_value (partitioning)
  - hash_array_extended (array hashing)
  - JsonbHashScalarValueExtended (JSONB hashing)
  - DatumGetFullTransactionId (transaction ID handling)

## Notes and Other Information
- This function provides the same platform abstraction as DatumGetInt64 but for unsigned values
- The USE_FLOAT8_BYVAL flag determines the storage method based on platform capabilities
- Commonly used in hashing and cryptographic contexts where unsigned arithmetic is preferred
- Essential for operations requiring the full 64-bit unsigned range (0 to 18,446,744,073,709,551,615)
- Used extensively in hash functions that need to combine multiple hash values
- The implementation ensures optimal performance on both 32-bit and 64-bit platforms