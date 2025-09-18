# SearchPathMatcher

## Location
[src/include/catalog/namespace.h:59-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/namespace.h#L59-L65)

## Overview
A structure that represents and efficiently matches PostgreSQL's schema search path configuration, enabling quick detection of search path changes through a generation counter mechanism.

## Definition


## Detailed Description
This structure captures a snapshot of PostgreSQL's schema search path configuration at a specific point in time. It's designed for efficient comparison with the current active search path to determine if cached plans or other search-path-dependent objects are still valid. The structure separates the explicitly named schemas from the implicitly added system schemas (pg_catalog and temporary schema).

The generation counter provides a fast-path optimization: when the search path hasn't changed, equality can be determined by simply comparing generation numbers rather than walking through the entire schema list. This is particularly important for performance since search path validation is checked frequently in query planning and execution.

## Parameters / Member Variables
- : List of explicitly named schema OIDs in the search path (excludes implicit pg_catalog and temp schemas)
- : Boolean flag indicating whether pg_catalog should be implicitly prepended to the search path
- : Boolean flag indicating whether the temporary schema should be implicitly prepended to the search path
- : Generation counter that matches the active path's generation when this matcher was last validated

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (PostgreSQL's generic list structure)
- Called from (representative examples):
  - [GetSearchPathMatcher](../G/GetSearchPathMatcher.md) (creates and populates the structure)
  - [CopySearchPathMatcher](../C/CopySearchPathMatcher.md) (copies the structure)
  - [SearchPathMatchesCurrentEnvironment](SearchPathMatchesCurrentEnvironment.md) (validates against current search path)
  - [SetTempNamespaceState](SetTempNamespaceState.md) (used in temporary namespace management)

## Notes and Other Information
- The generation counter is private to namespace.c and should not be modified by external code
- Can be initialized with generation = 0 to indicate "not known equal to current active path"
- Used extensively in plan caching (CachedPlanSource) to detect when plans need revalidation due to search path changes
- The structure separates implicit schemas (temp, pg_catalog) from explicit ones for more precise matching logic
- Memory allocation for the structure and its contents should be done in an appropriate memory context
- The fast-path generation comparison makes repeated search path validation very efficient in the common case where the search path remains stable