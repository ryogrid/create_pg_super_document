# PQresultAlloc

## Location
src/interfaces/libpq/fe-exec.c: 543 - 562

## Overview
PQresultAlloc is an exported routine that allocates local storage within a PGresult object, ensuring memory alignment for potential binary data.

## Definition


## Detailed Description
PQresultAlloc is a public API function that provides a safe interface for allocating memory within a PGresult structure. It serves as a wrapper around the internal pqResultAlloc function, forcing all allocations to be maxaligned since the function cannot determine whether the allocated memory will store binary data. The function includes safety checks to prevent allocation on NULL or out-of-memory PGresult objects.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure where memory should be allocated
- `nBytes`: Size in bytes of the memory block to allocate

## Dependencies
- Functions called/Symbols referenced:
  - pqResultAlloc
- Called from (representative examples):
  - PQsetResultAttrs

## Notes and Other Information
- All allocations are forced to maxaligned boundaries for binary data compatibility
- Returns NULL if the input PGresult is NULL or represents an OOM_result
- This is the public interface that client applications should use for PGresult memory allocation
- Located at src/interfaces/libpq/fe-exec.c:543-562