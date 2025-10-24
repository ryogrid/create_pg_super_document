# HashStmt

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:379-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L379-L414)

## Overview
Computes a hash value for SQL statements to determine their bucket position in the ECPG prepared statement cache.

## Definition
```c
static int HashStmt(const char *ecpgQuery)
```

## Detailed Description
This function implements a hash algorithm specifically designed for ECPG prepared statement caching. It processes up to the first 50 characters of a SQL statement to generate a hash value, then maps this to a specific bucket in the statement cache. The algorithm uses a 64-bit hash value with left rotation to distribute statements evenly across cache buckets. The function ensures that array entry 0 is never used by adding 1 to the final result.

## Parameters / Member Variables
- `ecpgQuery`: The SQL statement string to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - stmtCacheNBuckets (global variable)
  - stmtCacheEntPerBucket (global variable)
- Called from (representative examples):
  - [SearchStmtCache](../S/SearchStmtCache.md)
  - [AddStmtToCache](../A/AddStmtToCache.md)

## Notes and Other Information
- Uses only the first 50 characters of the SQL statement for hashing to balance performance and uniqueness
- Implements a 64-bit hash with 13-bit left rotation for better distribution
- The hash algorithm is designed to minimize collisions in the prepared statement cache
- Returns a bucket entry number (1-based indexing) to avoid using array position 0
- [Hash](Hash.md) collision handling is managed by the cache search and insertion functions that call this
- Static function - only accessible within the prepare.c compilation unit

## Simplified Source

```c
static int HashStmt(const char *ecpgQuery) {
    // Use first 50 characters for hashing
    int stmtLength = strlen(ecpgQuery);
    int hashLength = (stmtLength < 50) ? stmtLength : 50;

    // Generate 64-bit hash with rotation
    uint64 hashValue = 0;
    for (int i = 0; i < hashLength; i++) {
        hashValue += (unsigned char) ecpgQuery[i];

        // Rotate left by 13 bits for better distribution
        hashValue = hashValue << 13;
        uint64 rotatedBits = (hashValue & UINT64CONST(0x1fff00000000)) >> 32;
        hashValue = (hashValue & UINT64CONST(0xffffffff)) | rotatedBits;
    }

    // Map to bucket and ensure 1-based indexing
    int bucketNumber = hashValue % stmtCacheNBuckets;
    return bucketNumber * stmtCacheEntPerBucket + 1;
}
```