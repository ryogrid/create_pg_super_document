# hash_record

## Location
src/backend/utils/adt/rowtypes.c: 1794 - 1913

## Overview
The hash_record function computes a hash value for a composite type (record) by combining hash values of all its non-dropped columns.

## Definition


## Detailed Description
This function implements hash computation for PostgreSQL record (composite) types. It extracts the tuple structure from the input record, decomposes it into individual column values, and computes a combined hash by calling the appropriate hash function for each column's data type. The function uses caching mechanisms to avoid repeated lookups of type information and hash function details across multiple calls on the same record type.

The hash computation follows the same algorithm as hash_array(), using a left-shift and subtraction pattern:  for each column. NULL values contribute a hash of 0 to the final result.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing the record to be hashed

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetTypeId: Extract type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extract type modifier from tuple header  
  - lookup_rowtype_tupdesc: Get tuple descriptor for the record type
  - heap_deform_tuple: Break down tuple into individual column values
  - lookup_type_cache: Get type cache entry with hash function info
  - FunctionCallInvoke: Call the hash function for each column
  - check_stack_depth: Prevent stack overflow in recursive calls
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Uses RecordCompareData structure for caching type information and hash function details between calls
- Handles dropped columns by skipping them during hash computation
- Includes stack depth checking to prevent infinite recursion when hashing nested record types
- Memory management includes cleanup of temporary allocations and handling of toasted input
- Returns a 32-bit unsigned integer hash value
- Located in src/backend/utils/adt/rowtypes.c:1794-1913