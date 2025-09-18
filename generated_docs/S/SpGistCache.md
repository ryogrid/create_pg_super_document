# SpGistCache

## Location
src/include/access/spgist_private.h: 251 - 261

## Overview
SpGistCache is a structure that represents the cached metadata and configuration information for a SP-GiST (Space-Partitioned Generalized Search Tree) index, stored in the index's rd_amcache field.

## Definition


## Detailed Description
SpGistCache serves as the primary cache structure for SP-GiST indexes, combining both static configuration data and dynamic page caching information. This structure is maintained per-index and provides efficient access to frequently used metadata without requiring repeated lookups. The cache includes operator class configuration details, type descriptors for different tuple components, and a last-used pages cache for performance optimization.

## Parameters / Member Variables
- : Configuration output from the operator class config method, containing index-specific parameters
- : Type descriptor for the main values being indexed and restored from the index
- : Type descriptor specifically for leaf tuple values in the SP-GiST structure
- : Type descriptor for prefix values stored in inner tuples
- : Type descriptor for node label values used in tree navigation
- : Cache maintaining recently accessed pages to optimize buffer management

## Dependencies
- Functions called/Symbols referenced:
  - spgConfigOut
  - SpGistTypeDesc
  - SpGistLUPCache
- Called from (representative examples):
  - spgcanreturn
  - fillTypeDesc
  - spgGetCache
  - initSpGistState
  - SpGistUpdateMetaPage
  - allocNewBuffer
  - SpGistGetBuffer
  - SpGistSetLastUsedPage

## Notes and Other Information
- This structure is stored in index->rd_amcache for persistent caching across operations
- The cache significantly improves performance by avoiding repeated operator class configuration calls
- The lastUsedPages component implements a local caching strategy for recently accessed buffer pages
- Type descriptors are essential for proper serialization/deserialization of different tuple components in the SP-GiST tree structure