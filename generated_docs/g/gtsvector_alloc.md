# gtsvector_alloc

## Location
[src/backend/utils/adt/tsgistidx.c:156-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsgistidx.c#L156-L171)

## Overview
A static utility function that allocates and initializes a SignTSVector structure with specified flags, length, and optional signature data.

## Definition
static SignTSVector *gtsvector_alloc(int flag, int len, BITVECP sign)

## Detailed Description
The gtsvector_alloc function is responsible for allocating memory and initializing SignTSVector structures used in PostgreSQL's GiST indexing system for tsvector. It calculates the required memory size based on the flag and length parameters, allocates the memory using palloc, and sets up the structure with the appropriate variable-length header, flag field, and optional signature data. When the SIGNKEY flag is set (but not ALLISTRUE) and a signature is provided, it copies the signature data into the allocated structure. This function serves as a centralized memory allocation point for creating various types of GiST signature structures used throughout the tsvector indexing operations.

## Parameters / Member Variables
- : Integer flag indicating the type of SignTSVector to create (SIGNKEY, ALLISTRUE, etc.)
- : Length parameter used for size calculations (signature length or element count depending on flag)
- : Optional bit vector pointer containing signature data to copy (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - BITVECP (type definition for bit vector pointer)
  - CALCGTSIZE (macro to calculate required structure size based on flag and length)
  - [SignTSVector](../S/SignTSVector.md) (data type for GiST signature representation)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - SET_VARSIZE (macro to set variable-length structure size)
  - SIGNKEY (flag constant for signature key type)
  - ALLISTRUE (flag constant for all-true signature type)
  - GETSIGN (macro to get signature data pointer)
  - memcpy (memory copy function)
- Called from (representative examples):
  - [gtsvector_compress](gtsvector_compress.md) (during index compression operations)
  - [gtsvector_union](gtsvector_union.md) (when creating union signatures)
  - [gtsvector_picksplit](gtsvector_picksplit.md) (during index page splitting operations)

## Notes and Other Information
- This is a static function, accessible only within the tsgistidx.c file
- Handles variable-length PostgreSQL structures with proper VARSIZE management
- Conditionally copies signature data only for SIGNKEY structures (not ALLISTRUE)
- Central allocation point that ensures consistent initialization of SignTSVector structures
- Part of the GiST indexing infrastructure for tsvector full-text search functionality
- The function abstracts away the complexity of calculating proper structure sizes
- Memory allocated by this function must be managed according to PostgreSQL's memory context rules