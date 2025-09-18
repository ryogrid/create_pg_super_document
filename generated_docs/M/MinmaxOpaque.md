# MinmaxOpaque

## Location
src/backend/access/brin/brin_minmax.c: 23 - 27

## Overview
MinmaxOpaque is a private data structure used by BRIN (Block Range INdex) min/max operator classes to cache operator procedure information and maintain performance optimization for repeated lookups.

## Definition


## Detailed Description
MinmaxOpaque serves as an opaque (private) data structure within the BRIN min/max operator class implementation. It is used to cache frequently accessed procedure information to avoid repetitive system catalog lookups, which significantly improves performance during index operations.

The struct is allocated as part of a BrinOpcInfo structure and is accessed through the oi_opaque pointer. It maintains cached operator procedures for different comparison strategies (less than, less than or equal, equal, greater than or equal, greater than) used in min/max range comparisons.

The caching mechanism works by storing procedures for a specific subtype. When the subtype changes, all cached entries are invalidated and must be reloaded from the system catalogs.

## Parameters / Member Variables
- : The OID of the data type subtype for which the strategy procedures are currently cached. Used to determine when cache invalidation is necessary.
- : Array of FmgrInfo structures containing cached operator procedure information for each B-tree strategy number (1-5). BTMaxStrategyNumber is defined as 5, corresponding to the five standard comparison operators.

## Dependencies
- Functions called/Symbols referenced:
  - BTMaxStrategyNumber (constant defining maximum strategy number as 5)
- Called from (representative examples):
  - brin_minmax_opcinfo (allocates and initializes MinmaxOpaque)
  - minmax_get_strategy_procinfo (accesses cached procedure information)

## Notes and Other Information
- This structure is internal to the BRIN min/max implementation and should not be accessed directly by external code
- The strategy_procinfos array is initialized lazily using palloc0, which sets all FmgrInfo.fn_oid fields to InvalidOid initially
- Cache invalidation occurs when the subtype changes, ensuring correct procedure lookups for different data types
- The structure is part of PostgreSQL's BRIN indexing system, which provides efficient indexing for very large tables by maintaining min/max ranges for blocks of data
- Located in src/backend/access/brin/brin_minmax.c:23-27