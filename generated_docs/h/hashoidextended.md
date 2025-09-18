# hashoidextended

## Location
[src/backend/access/hash/hashfunc.c:122-127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L122-L127)

## Overview
The hashoidextended function computes an extended hash value for PostgreSQL's OID (Object Identifier) data type using an additional 64-bit seed value for enhanced hash distribution.

## Definition
Datum hashoidextended(PG_FUNCTION_ARGS)

## Detailed Description
This function provides extended hash functionality for PostgreSQL's OID data type, which is used to uniquely identify database objects. It extends the basic hashoid function by incorporating a 64-bit seed value to produce more robust hash values with better collision resistance. The function extracts the OID value, casts it to a 32-bit unsigned integer (since OIDs are internally 32-bit values), and passes it along with the seed value to the hash_uint32_extended function. This extended hashing capability is particularly useful in scenarios like hash joins where additional entropy helps reduce hash collisions.

## Parameters / Member Variables
- First argument (retrieved via PG_GETARG_OID(0)): The OID value to be hashed
- Second argument (retrieved via PG_GETARG_INT64(1)): A 64-bit seed value used to extend the hash computation

## Dependencies
- Functions called/Symbols referenced:
  - [hash_uint32_extended](hash_uint32_extended.md): Extended 32-bit hash function that incorporates the seed value
  - PG_GETARG_INT64: PostgreSQL macro to extract int64 argument from function call
  - PG_GETARG_OID: PostgreSQL macro to extract OID argument from function call (implicitly used)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- Provides enhanced hash distribution properties for OID values when additional entropy is needed
- The cast from OID to uint32 is safe since both types have the same 32-bit unsigned integer representation
- Part of PostgreSQL's extended hash function family that supports seed-based hashing
- Located in src/backend/access/hash/hashfunc.c:122-127
- Enables OID columns to participate in hash-based operations requiring collision-resistant hashing
- Typically used internally by PostgreSQL's query execution engine rather than being called directly