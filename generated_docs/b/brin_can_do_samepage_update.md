# brin_can_do_samepage_update

## Location
[src/backend/access/brin/brin_pageops.c:323-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_pageops.c#L323-L341)

## Overview
Determines whether a BRIN tuple update can be performed on the same page by checking if there is sufficient free space to accommodate the size difference between old and new tuples.

## Definition

```c
bool
brin_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz)
```
## Detailed Description
This utility function performs a simple but critical space calculation to determine if an in-place tuple update is feasible on the same page. The function implements an optimization check that avoids the overhead of cross-page updates when possible.

The logic is straightforward: if the new tuple is smaller than or equal to the original tuple size, the update can always be done in-place. If the new tuple is larger, the function checks whether the page has enough exact free space to accommodate the size difference.

This function is typically called before attempting a same-page update to avoid unnecessary work and ensure atomic operations can be completed successfully.

## Parameters / Member Variables
- : Buffer containing the page where the update would be performed
- : Size of the original tuple that would be replaced
- : Size of the new tuple that would be inserted

## Dependencies
- Functions called/Symbols referenced:
  - : Gets the exact amount of free space available on the page
  - : Extracts the page from the buffer
- Called from (representative examples):
  - : During BRIN tuple insertion operations
  - : When updating range summaries
  - : Before attempting same-page updates

## Notes and Other Information
- Returns  if same-page update is possible,  otherwise
- Uses exact free space calculation rather than approximate to ensure reliability
- The function assumes the buffer is already locked by the caller
- This is a lightweight check that helps optimize BRIN update performance by avoiding unnecessary page splits
- The calculation accounts for tuple size changes, allowing both shrinking and growing updates when space permits

## Simplified Source

```c
bool brin_can_do_samepage_update(Buffer buffer, Size origsz, Size newsz) {
    // If new tuple is smaller or same size, always fits
    if (newsz <= origsz) {
        return true;
    }

    // If new tuple is larger, check if page has enough free space for the difference
    Size space_needed = newsz - origsz;
    Size available_space = PageGetExactFreeSpace(BufferGetPage(buffer));

    return available_space >= space_needed;
}
```