# FreePageManagerDump

## Location
[src/backend/utils/mmgr/freepage.c:424-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/freepage.c#L424-L500)

## Overview
Produces a detailed debugging dump of the internal state of a free page manager for diagnostic purposes.

## Definition

```c
char *
FreePageManagerDump(FreePageManager *fpm)
```
## Detailed Description
This debugging function generates a comprehensive textual representation of the free page manager's internal state. The output includes all major data structures and their current contents, making it invaluable for troubleshooting memory management issues.

The dump includes:
1. **Metadata**: Self-pointer offset and maximum contiguous pages available
2. **B-tree structure**: If a B-tree exists (depth > 0), dumps the entire tree structure via recursive calls to 
3. **Singleton information**: For simple cases where only one free span exists
4. **Recycle list**: B-tree nodes available for reuse
5. **Free lists**: All non-empty freelists showing available page spans

The function constructs the output using PostgreSQL's StringInfo buffer mechanism and returns a dynamically allocated string that the caller must free.

## Parameters / Member Variables
- `*fpm`: Pointer to the FreePageManager structure to dump
## Dependencies
- Functions called/Symbols referenced:
  - fpm_segment_base
  - [FreePageSpanLeader](FreePageSpanLeader.md) (struct type)
  - [FreePageBtree](FreePageBtree.md) (struct type)
  - relptr_access
  - [FreePageManagerDumpBtree](FreePageManagerDumpBtree.md)
  - [FreePageManagerDumpSpans](FreePageManagerDumpSpans.md)
  - FPM_NUM_FREELISTS (constant)
- Called from (representative examples):
  - fpm_largest (likely a debugging/testing function)

## Notes and Other Information
This is a debugging utility function that provides human-readable output for analyzing the internal state of the free page manager. The returned string is dynamically allocated and must be freed by the caller. The function is primarily useful during development, testing, and troubleshooting memory management issues. The output format is designed to be readable and includes hierarchical indentation for complex structures like B-trees.

## Simplified Source

```c
char *FreePageManagerDump(FreePageManager *fpm) {
    char *base = fpm_segment_base(fpm);
    StringInfoData buf;
    FreePageSpanLeader *recycle;
    bool dumped_any_freelist = false;
    Size f;

    // Initialize output buffer
    initStringInfo(&buf);

    // Dump metadata information
    appendStringInfo(&buf, "metadata: self %zu max contiguous pages = %zu\n",
                     relptr_offset(fpm->self), fpm->contiguous_pages);

    // Dump btree structure if it exists
    if (fpm->btree_depth > 0) {
        FreePageBtree *root;
        appendStringInfo(&buf, "btree depth %u:\n", fpm->btree_depth);
        root = relptr_access(base, fpm->btree_root);
        FreePageManagerDumpBtree(fpm, root, NULL, 0, &buf);
    }
    else if (fpm->singleton_npages > 0) {
        appendStringInfo(&buf, "singleton: %zu(%zu)\n",
                         fpm->singleton_first_page, fpm->singleton_npages);
    }

    // Dump btree recycle list
    recycle = relptr_access(base, fpm->btree_recycle);
    if (recycle != NULL) {
        appendStringInfoString(&buf, "btree recycle:");
        FreePageManagerDumpSpans(fpm, recycle, 1, &buf);
    }

    // Dump all non-empty freelists
    for (f = 0; f < FPM_NUM_FREELISTS; ++f) {
        FreePageSpanLeader *span;

        if (relptr_is_null(fpm->freelist[f]))
            continue;
        if (!dumped_any_freelist) {
            appendStringInfoString(&buf, "freelists:\n");
            dumped_any_freelist = true;
        }
        appendStringInfo(&buf, "  %zu:", f + 1);
        span = relptr_access(base, fpm->freelist[f]);
        FreePageManagerDumpSpans(fpm, span, f + 1, &buf);
    }

    return buf.data;
}
```