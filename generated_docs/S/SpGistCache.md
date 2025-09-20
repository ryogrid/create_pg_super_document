# SpGistCache

## Location
[src/include/access/spgist_private.h:251-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/spgist_private.h#L251-L261)

## Overview
SpGistCache is a structure that represents the cached metadata and configuration information for a SP-GiST (Space-Partitioned Generalized Search Tree) index, stored in the index's rd_amcache field.

## Definition

```c
typedef struct SpGistCache
{
	spgConfigOut config;		/* filled in by opclass config method */

	SpGistTypeDesc attType;		/* type of values to be indexed/restored */
	SpGistTypeDesc attLeafType; /* type of leaf-tuple values */
	SpGistTypeDesc attPrefixType;	/* type of inner-tuple prefix values */
	SpGistTypeDesc attLabelType;	/* type of node label values */

	SpGistLUPCache lastUsedPages;	/* local storage of last-used info */
} SpGistCache;
```
## Detailed Description
SpGistCache serves as the primary cache structure for SP-GiST indexes, combining both static configuration data and dynamic page caching information. This structure is maintained per-index and provides efficient access to frequently used metadata without requiring repeated lookups. The cache includes operator class configuration details, type descriptors for different tuple components, and a last-used pages cache for performance optimization.

## Parameters / Member Variables
- `config`: Configuration output from the operator class config method, containing index-specific parameters
- `attType`: Type descriptor for the main values being indexed and restored from the index
- `attLeafType`: Type descriptor specifically for leaf tuple values in the SP-GiST structure
- `attPrefixType`: Type descriptor for prefix values stored in inner tuples
- `attLabelType`: Type descriptor for node label values used in tree navigation
- `lastUsedPages`: Cache maintaining recently accessed pages to optimize buffer management
## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](../s/spgConfigOut.md)
  - SpGistTypeDesc
  - [SpGistLUPCache](SpGistLUPCache.md)
- Called from (representative examples):
  - [spgcanreturn](../s/spgcanreturn.md)
  - [fillTypeDesc](../f/fillTypeDesc.md)
  - [spgGetCache](../s/spgGetCache.md)
  - [initSpGistState](../i/initSpGistState.md)
  - [SpGistUpdateMetaPage](SpGistUpdateMetaPage.md)
  - [allocNewBuffer](../a/allocNewBuffer.md)
  - [SpGistGetBuffer](SpGistGetBuffer.md)
  - [SpGistSetLastUsedPage](SpGistSetLastUsedPage.md)

## Notes and Other Information
- This structure is stored in index->rd_amcache for persistent caching across operations
- The cache significantly improves performance by avoiding repeated operator class configuration calls
- The lastUsedPages component implements a local caching strategy for recently accessed buffer pages
- Type descriptors are essential for proper serialization/deserialization of different tuple components in the SP-GiST tree structure