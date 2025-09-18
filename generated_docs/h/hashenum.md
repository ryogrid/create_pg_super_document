# hashenum

## Location
src/backend/access/hash/hashfunc.c: 128 - 133

## Overview
A PostgreSQL hash function that computes a hash value for enumeration type values by treating them as 32-bit unsigned integers.

## Definition


## Detailed Description
The  function is a specialized hash function for PostgreSQL enumeration types. It extracts the enumeration value as an OID (Object Identifier) using , casts it to a 32-bit unsigned integer, and delegates the actual hash computation to the  function. This approach treats enumeration values as their underlying integer representations for hashing purposes, ensuring consistent hash values for the same enumeration value.

## Parameters / Member Variables
- Uses PostgreSQL's function argument macros ()
- Accesses the first argument as an OID via 

## Dependencies
- Functions called/Symbols referenced:
  - hash_uint32
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically used as a hash function in hash indexes or hash joins involving enumeration types
- The function converts the enumeration OID to a uint32 before hashing, ensuring compatibility with the standard 32-bit integer hash function
- Located in src/backend/access/hash/hashfunc.c at lines 128-133