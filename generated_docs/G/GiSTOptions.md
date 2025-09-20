# GiSTOptions

## Location
[src/include/access/gist_private.h:394-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gist_private.h#L394-L399)

## Overview
GiSTOptions is a storage structure for GiST index configuration options, containing settings that control index behavior such as page fill factor and buffering mode during index operations.

## Definition

```c
typedef struct GiSTOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			fillfactor;		/* page fill factor in percent (0..100) */
	GistOptBufferingMode buffering_mode;	/* buffering build mode */
} GiSTOptions;
```
## Detailed Description
GiSTOptions serves as the storage type for GiST index reloptions (relation options), encapsulating configuration parameters that control various aspects of GiST index behavior. The structure follows PostgreSQL's varlena format, allowing it to be stored as variable-length data. The options control performance-related settings such as how full pages should be before splitting (fillfactor) and what buffering strategy to use during index construction (buffering_mode). These options can be specified when creating or altering GiST indexes.

## Parameters / Member Variables
- : int32 varlena header field required for variable-length data structures (should not be accessed directly)
- : Integer representing page fill factor as a percentage (0-100), controlling how full pages become before splitting
- : GistOptBufferingMode enumeration value specifying the buffering strategy used during index construction

## Dependencies
- Functions called/Symbols referenced:
  - GistOptBufferingMode
- Called from (representative examples):
  - [gistbuild](../g/gistbuild.md)
  - [gistoptions](../g/gistoptions.md)

## Notes and Other Information
The GiSTOptions structure is part of PostgreSQL's reloption system, which allows users to customize index behavior through CREATE INDEX and ALTER INDEX statements. The fillfactor option is particularly important for performance tuning, as it affects the balance between storage efficiency and update performance. Lower fill factors leave more free space for insertions but use more storage, while higher fill factors pack data more densely but may cause more page splits during updates. The buffering_mode option controls memory usage and I/O patterns during index construction, which can significantly impact build performance for large indexes.