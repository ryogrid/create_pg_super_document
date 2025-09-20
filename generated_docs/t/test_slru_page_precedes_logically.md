# test_slru_page_precedes_logically

## Location
[src/test/modules/test_slru/test_slru.c:208-213](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_slru/test_slru.c#L208-L213)

## Overview
A comparison function that determines the logical ordering of SLRU pages by comparing their page numbers for the test SLRU module.

## Definition

```c
static bool
test_slru_page_precedes_logically(int64 page1, int64 page2)
```
## Detailed Description
This function implements a simple logical ordering comparison for SLRU (Simple LRU) pages in the test module. It serves as a callback function that is assigned to the PagePrecedes field of the SLRU control structure. The function determines whether one page logically precedes another by performing a simple numerical comparison of their page numbers. This ordering is used by the SLRU system to determine page precedence for various operations like truncation and logical consistency checks.

## Parameters / Member Variables
- `page1`: The first page number to compare (int64)
- `page2`: The second page number to compare (int64)

## Return Value
- Returns `true` if page1 precedes page2 logically (page1 < page2)
- Returns `false` otherwise

## Dependencies
- Functions called/Symbols referenced:
  - None (pure comparison function)
- Called from (representative examples):
  - Used as callback in TestSlruCtl->PagePrecedes assignment in test_slru_shmem_startup

## Notes and Other Information
- This is a static function, meaning it's only accessible within the test_slru.c file
- Implements a simple monotonic ordering based on page numbers
- This callback function is required by the SLRU system to understand the logical ordering of pages
- The function pointer is assigned to TestSlruCtl->PagePrecedes during SLRU initialization
- In production SLRU implementations, page precedence logic may be more complex depending on the specific use case