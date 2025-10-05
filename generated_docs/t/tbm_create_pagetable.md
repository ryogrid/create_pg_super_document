# tbm_create_pagetable

## Location
[src/backend/nodes/tidbitmap.c:292-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L292-L321)

## Overview
Creates the internal hash table structure for a TID bitmap when it transitions from storing a single page entry to needing multiple page entries.

## Definition

```c
static void
tbm_create_pagetable(TIDBitmap *tbm)
```
## Detailed Description
The `tbm_create_pagetable` function performs a lazy initialization of the hash table structure within a TID bitmap. This is called when the bitmap needs to transition from the TBM_ONE_PAGE state (storing a single page worth of tuple identifiers) to the TBM_HASH state (storing multiple pages in a hash table). The function is marked as static since it's an internal implementation detail of the TID bitmap system.

The function creates the hash table with an initial size of 128 buckets and transfers any existing single page entry into the new hash table structure. This lazy approach avoids the overhead of creating a hash table until it's actually needed.

## Parameters / Member Variables
- `tbm`: Pointer to the TIDBitmap structure that needs a hash table created

## Dependencies
- Functions called/Symbols referenced:
  - pagetable_create
  - pagetable_insert
  - memcpy
  - Assert (macro)
  - TBM_HASH (enum value)
  - TBM_ONE_PAGE (enum value)
  - [PagetableEntry](../P/PagetableEntry.md) (struct type)
- Called from (representative examples):
  - [tbm_get_pageentry](tbm_get_pageentry.md)
  - [tbm_mark_page_lossy](tbm_mark_page_lossy.md)

## Notes and Other Information
- This is a static (internal) function, not part of the public TID bitmap API
- Creates hash table with initial size of 128 buckets for reasonable performance
- Handles migration of existing single page entry (entry1) into the new hash table
- Updates bitmap status from TBM_ONE_PAGE to TBM_HASH after successful creation
- Uses assertions to ensure the bitmap is in a valid state before creating the hash table
- The expensive hash table creation is deferred until actually needed (lazy initialization)

## Simplified Source

```c
static void tbm_create_pagetable(TIDBitmap *tbm) {
    Assert(tbm->status != TBM_HASH);
    Assert(tbm->pagetable == NULL);

    // Create hash table with 128 initial buckets
    tbm->pagetable = pagetable_create(tbm->mcxt, 128, tbm);

    // If we had a single page entry, migrate it to the hash table
    if (tbm->status == TBM_ONE_PAGE) {
        PagetableEntry *page;
        bool found;

        page = pagetable_insert(tbm->pagetable, tbm->entry1.blockno, &found);
        Assert(!found);

        // Copy the single entry data to the hash table
        char oldstatus = page->status;
        memcpy(page, &tbm->entry1, sizeof(PagetableEntry));
        page->status = oldstatus;
    }

    // Transition to hash table mode
    tbm->status = TBM_HASH;
}
```