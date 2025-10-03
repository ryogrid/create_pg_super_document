# _hash_checkpage

## Location
[src/backend/access/hash/hashutil.c:210-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L210-L274)

## Overview
Performs comprehensive sanity checks on hash index pages to detect corruption and ensure structural integrity.

## Definition
```c
void _hash_checkpage(Relation rel, Buffer buf, int flags)
```

## Detailed Description
This function validates the format and integrity of hash index pages, serving as a critical defensive mechanism against index corruption. It performs multiple levels of validation:

1. **Basic Page Structure**: Checks for unexpected zero pages that would indicate uninitialized or corrupted storage
2. **Special Area Validation**: Verifies that the page's special area has the correct size for hash page metadata
3. **Page Type Validation**: When flags are specified, ensures the page matches one of the expected hash page types
4. **Metapage-Specific Checks**: For metapages, validates the magic number and version to confirm proper hash index format

The function reports detailed error messages with hints for recovery (typically REINDEX) when corruption is detected, making it essential for maintaining hash index reliability.

## Parameters
- `rel`: The relation (hash index) being checked
- `buf`: Buffer containing the page to validate
- `flags`: Bitwise OR of acceptable page types (LH_PAGE_TYPE values), or 0 to skip type checking

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIsNew](../P/PageIsNew.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [PageGetSpecialSize](../P/PageGetSpecialSize.md)
  - HashPageGetOpaque
  - HashPageGetMeta
  - HASH_MAGIC (constant)
  - HASH_VERSION (constant)
  - LH_META_PAGE (constant)
- Called from (representative examples):
  - [hashbulkdelete](hashbulkdelete.md)
  - [_hash_pgaddtup](_hash_pgaddtup.md)
  - [_hash_getbuf](_hash_getbuf.md)
  - [_hash_expandtable](_hash_expandtable.md)
  - [_hash_readpage](_hash_readpage.md)

## Notes and Other Information
This function is called extensively throughout hash index operations as a protective measure. It's particularly important during recovery scenarios and when debugging index corruption issues. The function distinguishes between different types of corruption (structural vs. content-based) and provides appropriate error messages and recovery suggestions for each case.

## Simplified Source

```c
void _hash_checkpage(Relation rel, Buffer buf, int flags)
{
    Page page = BufferGetPage(buf);

    // Check for uninitialized pages
    if (PageIsNew(page))
        ereport(ERROR, "index contains unexpected zero page - please REINDEX");

    // Verify special area has correct size for hash pages
    if (PageGetSpecialSize(page) != MAXALIGN(sizeof(HashPageOpaqueData)))
        ereport(ERROR, "index contains corrupted page - please REINDEX");

    // Check page type if flags specified
    if (flags)
    {
        HashPageOpaque opaque = HashPageGetOpaque(page);
        if ((opaque->hasho_flag & flags) == 0)
            ereport(ERROR, "index contains corrupted page - please REINDEX");
    }

    // Additional checks for metapages
    if (flags == LH_META_PAGE)
    {
        HashMetaPage metap = HashPageGetMeta(page);

        if (metap->hashm_magic != HASH_MAGIC)
            ereport(ERROR, "index is not a hash index");

        if (metap->hashm_version != HASH_VERSION)
            ereport(ERROR, "index has wrong hash version - please REINDEX");
    }
}
```