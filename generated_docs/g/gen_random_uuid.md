# gen_random_uuid

## Location
[src/backend/utils/adt/uuid.c:411-429](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L411-L429)

## Overview
A SQL function that generates a random UUID (version 4) using cryptographically strong random number generation according to RFC 4122 specifications.

## Definition
```c
Datum gen_random_uuid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements PostgreSQL's built-in capability to generate random UUIDs (Universally Unique Identifiers) of version 4 according to RFC 4122 standards. It creates a 128-bit identifier where most bits are randomly generated using cryptographically strong random number generation, with specific version and variant bits set according to the standard.

The function performs the following operations:
1. **Memory allocation**: Allocates space for a 16-byte UUID structure
2. **Random generation**: Fills the UUID with cryptographically strong random bytes using `pg_strong_random`
3. **Version setting**: Sets the version field (bits 12-15 of time_hi_and_version) to 4 (0100 binary)
4. **Variant setting**: Sets the variant field (bits 6-7 of clock_seq_hi_and_reserved) to 10 binary

The resulting UUID has the format xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx where x represents random hex digits and y represents random hex digits with the first bit set to 1.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Takes no input parameters
- Returns a newly generated random UUID

## Dependencies
- Functions called/Symbols referenced:
  - [pg_uuid_t](../p/pg_uuid_t.md) (UUID structure type)
  - UUID_LEN (UUID length constant - 16 bytes)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [pg_strong_random](../p/pg_strong_random.md) (cryptographic random number generator)
  - ereport/ERROR (error reporting macros)
  - [errcode](../e/errcode.md)/ERRCODE_INTERNAL_ERROR (error code definitions)
  - [errmsg](../e/errmsg.md) (error message function)
  - PG_RETURN_UUID_P (return value macro)
- Called from (representative examples):
  - SQL queries using gen_random_uuid() function
  - Applications requiring unique identifier generation

## Notes and Other Information
- Generates RFC 4122 compliant version 4 (random) UUIDs
- Uses PostgreSQL's cryptographically secure random number generator
- Throws an error if random number generation fails, ensuring no weak UUIDs are produced
- The probability of generating duplicate UUIDs is astronomically low (approximately 1 in 2^122)
- Version 4 UUIDs are preferred for most applications due to their lack of dependency on network hardware or timestamps
- Available as a SQL function for use in queries, defaults, and stored procedures