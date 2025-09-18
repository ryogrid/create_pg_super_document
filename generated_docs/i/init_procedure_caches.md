# init_procedure_caches

## Location
src/pl/plpython/plpy_procedure.c: 33 - 48

## Overview
Initializes the global procedure cache hash table used by the PL/Python procedural language extension to store compiled Python procedures.

## Definition


## Detailed Description
This function sets up a hash table that serves as a cache for compiled PL/Python procedures. The cache improves performance by avoiding recompilation of Python functions that have already been processed. The hash table is configured with specific parameters for key size (PLyProcedureKey) and entry size (PLyProcedureEntry), and uses PostgreSQL's hash table infrastructure with ELEM and BLOBS hash options for efficient storage and retrieval of procedure objects.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - hash_create (PostgreSQL hash table creation function)
  - HASHCTL (hash table control structure)
  - PLyProcedureKey (key structure for procedure cache)
  - PLyProcedureEntry (entry structure for cached procedures)
  - HASH_ELEM (hash table flag for element-based hashing)
  - HASH_BLOBS (hash table flag for blob-based key comparison)
- Called from (representative examples):
  - PLy_initialize (main PL/Python initialization function)

## Notes and Other Information
- This function must be called during PL/Python initialization before any procedures are cached
- The hash table created has an initial size of 32 entries but can grow as needed
- The procedure cache is global and persists for the lifetime of the backend process
- Uses PostgreSQL's built-in hash table implementation for thread safety and memory management