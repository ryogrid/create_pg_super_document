# BuildSpeculativeIndexInfo

## Location
src/backend/catalog/index.c: 2642 - 2701

## Overview
BuildSpeculativeIndexInfo augments an IndexInfo structure with additional metadata required for speculative insertion operations on unique indexes.

## Definition


## Detailed Description
BuildSpeculativeIndexInfo extends an existing IndexInfo structure with specialized information needed to support speculative insertion in unique B-tree indexes. This function is specifically designed for PostgreSQL's speculative insertion mechanism, which allows for optimistic insertion followed by uniqueness checking. The function allocates and populates arrays for unique operators, procedure OIDs, and strategy numbers that are used during the speculative insertion process. This processing is done separately from BuildIndexInfo() to avoid overhead in common non-speculative cases, ensuring optimal performance for regular index operations.

## Parameters / Member Variables
- : Relation structure representing the index being prepared for speculative insertion
- : IndexInfo structure to be augmented with speculative insertion metadata

## Dependencies
- Functions called/Symbols referenced:
  - IndexInfo (structure type)
  - IndexRelationGetNumberOfKeyAttributes (function)
  - get_opfamily_member (function) 
  - get_opcode (function)
- Called from (representative examples):
  - ExecOpenIndices

## Notes and Other Information
- Only supports B-tree indexes (BTREE_AM_OID) and will error for other access methods
- Requires the index to be unique (asserted with ii->ii_Unique)
- Allocates memory for three arrays: ii_UniqueOps, ii_UniqueProcs, and ii_UniqueStrats
- Uses BTEqualStrategyNumber strategy for all key attributes
- Performs validation to ensure required operators exist in the opfamily
- This function is part of PostgreSQL's speculative insertion optimization that reduces lock contention during concurrent unique constraint checking