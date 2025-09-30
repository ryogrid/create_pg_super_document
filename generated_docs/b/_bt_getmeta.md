# _bt_getmeta

## Location
[src/backend/access/nbtree/nbtpage.c:142-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L142-L178)

## Overview
_bt_getmeta retrieves and validates metadata from a share-locked buffer containing a B-tree metapage, performing standard sanity checks to ensure index integrity.

## Definition
static BTMetaPageData *_bt_getmeta(Relation rel, Buffer metabuf)

## Detailed Description
This static function safely extracts metadata from a B-tree metapage while performing comprehensive validation checks. It first retrieves the page from the buffer and extracts both the opaque area and metadata. The function then performs critical sanity checks including verifying the page is actually a metapage (using P_ISMETA), checking the magic number matches BTREE_MAGIC, and ensuring the version number is within supported bounds (between BTREE_MIN_VERSION and BTREE_VERSION). If any validation fails, the function reports an INDEX_CORRUPTED error with detailed information. Callers should be aware that concurrent _bt_upgrademetapage() operations may modify version-specific fields without invalidating cached data.

## Parameters / Member Variables
- `rel`: The relation (index) being accessed
- `metabuf`: Buffer containing the metapage (must be share-locked)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - BTPageGetOpaque
  - BTPageGetMeta
  - P_ISMETA
  - BTREE_MAGIC
  - BTREE_MIN_VERSION
  - BTREE_VERSION
  - RelationGetRelationName
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - BTPageOpaque
  - [BTMetaPageData](../B/BTMetaPageData.md)
- Called from (representative examples):
  - [_bt_getroot](_bt_getroot.md)
  - [_bt_getrootheight](_bt_getrootheight.md)
  - [_bt_metaversion](_bt_metaversion.md)

## Notes and Other Information
- The function is static and only used internally within the nbtpage.c module
- Requires the metabuf to be share-locked by the caller for safe access
- Performs comprehensive validation to detect index corruption early
- Handles version compatibility checks for different B-tree format versions
- Callers caching returned data should account for potential on-the-fly upgrades
- Error messages include specific version information to aid debugging
- Returns a pointer to the metadata structure within the page buffer

## Simplified Source

```c
static BTMetaPageData *
_bt_getmeta(Relation rel, Buffer metabuf)
{
    Page metapg;
    BTPageOpaque metaopaque;
    BTMetaPageData *metad;

    // Extract page and metadata from buffer
    metapg = BufferGetPage(metabuf);
    metaopaque = BTPageGetOpaque(metapg);
    metad = BTPageGetMeta(metapg);

    // Verify this is actually a metapage with correct magic number
    if (!P_ISMETA(metaopaque) || metad->btm_magic != BTREE_MAGIC)
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("index \"%s\" is not a btree",
                        RelationGetRelationName(rel))));

    // Check version compatibility
    if (metad->btm_version < BTREE_MIN_VERSION ||
        metad->btm_version > BTREE_VERSION)
        ereport(ERROR,
                (errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("version mismatch in index \"%s\": file version %d, "
                        "current version %d, minimal supported version %d",
                        RelationGetRelationName(rel),
                        metad->btm_version, BTREE_VERSION, BTREE_MIN_VERSION)));

    return metad;
}
```