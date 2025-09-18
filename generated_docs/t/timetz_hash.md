# timetz_hash

## Location
[src/backend/utils/adt/date.c:2533-2548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2533-L2548)

## Overview
The timetz_hash function computes a hash value for a TimeTzADT (time with time zone) value, used for hash table operations and hash-based indexing.

## Definition
Datum timetz_hash(PG_FUNCTION_ARGS)

## Detailed Description
This function implements hash computation for the TimeTzADT data type, which is essential for hash-based operations like hash joins, hash aggregation, and hash indexing in PostgreSQL. The function takes a TimeTzADT value and produces a 32-bit unsigned integer hash.

To ensure consistent and reliable hashing, the function computes separate hash values for each field of the TimeTzADT structure and combines them using XOR. This approach avoids potential issues with padding bytes that might exist in the struct and ensures that the hash depends on both the time component and the timezone offset.

The hashing process:
1. Hashes the time component (int64) using the hashint8 function
2. Hashes the zone component (int32) using the hash_uint32 function  
3. XORs the two hash values together to produce the final result

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function argument structure containing:
  - Argument 0: TimeTzADT value (key) to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMETZADT_P: Extracts TimeTzADT argument from function call
  - [hashint8](../h/hashint8.md): Computes hash value for 64-bit integer (time component)
  - [hash_uint32](../h/hash_uint32.md): Computes hash value for 32-bit unsigned integer (zone component)
  - DirectFunctionCall1: PostgreSQL function call mechanism
  - [DatumGetUInt32](../D/DatumGetUInt32.md): Extracts uint32 value from Datum
  - Int64GetDatumFast: Converts int64 to Datum efficiently
  - PG_RETURN_UINT32: Returns uint32 result to PostgreSQL function call framework
- Data types used:
  - TimeTzADT: Structure containing time (TimeADT) and zone (int32) fields
- Called from (representative examples):
  - Hash table operations for timetz data
  - Hash-based indexing (hash indexes)
  - Hash joins involving timetz columns

## Notes and Other Information
- Essential for hash-based operations in PostgreSQL's query execution engine
- Uses XOR combination to avoid padding byte issues in the TimeTzADT struct
- Ensures that values with the same time and timezone will always produce the same hash
- The hash function must be consistent with the equality semantics of timetz_cmp
- Returns a PostgreSQL Datum containing an unsigned 32-bit integer hash value
- Located in src/backend/utils/adt/date.c:2533-2548
- Part of the type system infrastructure required for efficient hash-based algorithms