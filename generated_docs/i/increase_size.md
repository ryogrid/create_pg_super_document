# increase_size

## Location
[src/backend/snowball/libstemmer/utilities.c:355-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L355-L373)

## Overview
Increases the size of a dynamically allocated symbol buffer to accommodate at least n symbols, providing memory management for string manipulation operations in PostgreSQL's Snowball stemmer.

## Definition

```c
*/
static symbol * increase_size(symbol * p, int n)
```
## Detailed Description
The  function is a memory management utility in PostgreSQL's Snowball stemming library that resizes a symbol buffer to ensure it can hold at least  symbols. The function adds a 20-symbol padding to the requested size for efficiency and uses  to resize the memory block. The buffer includes a header section (HEAD) that stores metadata such as the buffer's capacity. If memory reallocation fails, the function automatically frees the original buffer using  and returns NULL to indicate failure.

## Parameters / Member Variables
- `*p`: Pointer to the existing symbol buffer to be resized
- `n`: Minimum number of symbols the buffer should accommodate
## Dependencies
- Functions called/Symbols referenced:
  - realloc
  - [lose_s](../l/lose_s.md)
  - HEAD (macro for header size)
  - CAPACITY (macro for accessing buffer capacity)
  - symbol (type definition)
- Called from (representative examples):
  - [replace_s](../r/replace_s.md)
  - [slice_to](../s/slice_to.md)
  - [assign_to](../a/assign_to.md)

## Notes and Other Information
- This is a static function, only accessible within the utilities.c file
- The function adds 20 extra symbols to the requested size for performance optimization
- Memory layout includes a header section before the actual symbol data
- On allocation failure, the original buffer is automatically freed to prevent memory leaks
- Part of PostgreSQL's Snowball stemming algorithm implementation for text processing