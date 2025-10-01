# bms_replace_members

## Location
[src/backend/nodes/bitmapset.c:972-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L972-L1018)

## Overview
Completely replaces the contents of one bitmapset with the contents of another, recycling the target bitmapset's memory when possible.

## Definition

```c
Bitmapset *
bms_replace_members(Bitmapset *a, const Bitmapset *b)
```
## Detailed Description
The  function efficiently replaces all members of bitmapset  with the members from bitmapset . Unlike creating a new bitmapset, this function attempts to reuse the existing memory allocation of  when possible. If  has sufficient capacity to hold all members of , it simply copies the word data directly. If more space is needed, it expands  using  before copying.

This function effectively performs a complete assignment operation () while trying to minimize memory allocations and deallocations. The original contents of  are completely overwritten.

## Parameters / Member Variables
- : Target Bitmapset to be replaced (can be NULL, which copies )
- : Source Bitmapset to copy members from (const, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates both input bitmapset structures
  - : Creates a copy of a bitmapset when  is NULL
  - : Macro to calculate memory size needed for given word count
  - : PostgreSQL memory reallocation function
  - : Optional copy-and-free operation (when REALLOCATE_BITMAPSETS is defined)
  - : PostgreSQL memory deallocation function
- Called from (representative examples):
  - : Knapsack algorithm implementation
  - Various utility functions that need complete set replacement

## Notes and Other Information
- Returns a copy of  when  is NULL
- Returns NULL and frees  when  is NULL
- Expands 's storage only when necessary (when )
- Copies all word data directly from  to  for maximum efficiency
- Updates the word count to match the source bitmapset
- Under  compile flag, performs additional copy-and-free for memory safety
- More efficient than freeing  and creating a copy of  when the sizes are similar
- Useful for algorithms that need to reset a working bitmapset to new contents
- Maintains memory locality better than allocation/deallocation patterns
- Less commonly used than other bitmapset operations, primarily in specialized algorithms

## Simplified Source

```c
Bitmapset *bms_replace_members(Bitmapset *a, const Bitmapset *b)
{
    int i;

    Assert(bms_is_valid_set(a));
    Assert(bms_is_valid_set(b));

    // Handle NULL cases
    if (a == NULL)
        return bms_copy(b);
    if (b == NULL) {
        pfree(a);
        return NULL;
    }

    // Expand 'a' if it's too small to hold 'b'
    if (a->nwords < b->nwords)
        a = (Bitmapset *) repalloc(a, BITMAPSET_SIZE(b->nwords));

    // Copy all words from 'b' to 'a'
    i = 0;
    do {
        a->words[i] = b->words[i];
    } while (++i < b->nwords);

    // Update word count to match source
    a->nwords = b->nwords;

#ifdef REALLOCATE_BITMAPSETS
    // Copy and free for memory safety when flag is enabled
    a = bms_copy_and_free(a);
#endif

    return a;
}
```