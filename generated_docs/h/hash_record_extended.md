# hash_record_extended

## Location
src/backend/utils/adt/rowtypes.c: 1914 - 2034

## Overview
The hash_record_extended function computes a seeded hash value for a composite type (record) using extended hash functions that accept a seed parameter for enhanced hash distribution.

## Definition


## Detailed Description
This function is the extended version of hash_record that supports seeded hashing. It takes both a record and a 64-bit seed value as input parameters. The function decomposes the record into individual columns and computes a combined hash by calling the extended hash function for each column's data type, passing the seed value to ensure better hash distribution and collision resistance.

Like its non-extended counterpart, it uses caching mechanisms to avoid repeated lookups of type information and hash function details. The hash computation follows the same left-shift algorithm: . The key difference is the use of TYPECACHE_HASH_EXTENDED_PROC_FINFO to access extended hash functions and the return of a 64-bit hash value instead of 32-bit.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - Argument 0: The record (HeapTupleHeader) to be hashed
  - Argument 1: The 64-bit seed value (int64) for hash computation

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId: Extract type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extract type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Get tuple descriptor for the record type
  - [heap_deform_tuple](heap_deform_tuple.md): Break down tuple into individual column values
  - [lookup_type_cache](../l/lookup_type_cache.md): Get type cache entry with extended hash function info
  - FunctionCallInvoke: Call the extended hash function for each column
  - check_stack_depth: Prevent stack overflow in recursive calls
  - [Int64GetDatum](../I/Int64GetDatum.md): Convert seed value to Datum for function calls
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Extended version of hash_record that supports seeded hashing for improved hash distribution
- Uses TYPECACHE_HASH_EXTENDED_PROC_FINFO instead of TYPECACHE_HASH_PROC_FINFO 
- Returns a 64-bit unsigned integer hash value instead of 32-bit
- Passes seed parameter to each column's extended hash function
- Includes the same caching, memory management, and error handling as hash_record
- Handles dropped columns by skipping them during hash computation
- Located in src/backend/utils/adt/rowtypes.c:1914-2034