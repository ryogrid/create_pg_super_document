# GinOptions

## Location
[src/include/access/gin_private.h:26-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gin_private.h#L26-L31)

## Overview
GinOptions is a storage structure that holds reloption (relation options) parameters for GIN (Generalized Inverted Index) indexes, controlling fast update behavior and pending list management.

## Definition

```c
typedef struct GinOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		useFastUpdate;	/* use fast updates? */
	int			pendingListCleanupSize; /* maximum size of pending list */
} GinOptions;
```
## Detailed Description
GinOptions stores configuration parameters that control the behavior of GIN indexes, particularly related to performance optimization features. It follows PostgreSQL's varlena structure format for relation options. The structure enables fine-tuning of GIN index operations through two key parameters: fast update mode and pending list size management.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header used by PostgreSQL's variable-length data types - should not be accessed directly
- `useFastUpdate`: Boolean flag that enables or disables fast update mode for the GIN index
- `pendingListCleanupSize`: Integer specifying the maximum size threshold for the pending list before cleanup operations are triggered
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - GinGetUseFastUpdate
  - GinGetPendingListCleanupSize
  - [ginoptions](../g/ginoptions.md) (function in ginutil.c)

## Notes and Other Information
- Located in src/include/access/gin_private.h:26-31
- Uses PostgreSQL's varlena format for storing relation options
- Fast updates allow GIN indexes to defer some maintenance work to improve insertion performance
- The pending list cleanup size controls when accumulated pending entries are merged into the main index structure