# spg_kd_config

## Location
[src/backend/access/spgist/spgkdtreeproc.c:28-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgkdtreeproc.c#L28-L40)

## Overview
Configuration function for SP-GiST k-dimensional tree (k-d tree) opclass that sets up the basic operational parameters for k-d tree indexing in PostgreSQL.

## Definition


## Detailed Description
This function serves as the configuration entry point for SP-GiST k-d tree operations. It initializes the  structure with parameters specific to k-dimensional tree indexing. The function sets up the data types used for prefixes and labels, specifies that the index can return data directly, and indicates that long values are not supported. This configuration is essential for establishing how the SP-GiST framework will handle k-d tree operations.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  - Input argument 0:  (unused in this implementation)  
  - Input argument 1:  - Output configuration structure to be populated

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](spgConfigOut.md) (structure type)
  - PG_RETURN_VOID (PostgreSQL macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is part of the SP-GiST (Space-Partitioned Generalized Search Tree) framework
- Sets  to FLOAT8OID, indicating the index works with double-precision floating-point data
- Sets  to VOIDOID since k-d trees don't require node labels
- Enables  allowing index-only scans when possible
- Disables  as k-d trees are optimized for fixed-size numeric data
- Located in src/backend/access/spgist/spgkdtreeproc.c:28-40