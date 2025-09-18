# increase_size

## Location
src/backend/snowball/libstemmer/utilities.c: 355 - 373

## Overview
Increases the size of a dynamically allocated symbol buffer to accommodate at least n symbols, providing memory management for string manipulation operations in PostgreSQL's Snowball stemmer.

## Definition


## Detailed Description
The  function is a memory management utility in PostgreSQL's Snowball stemming library that resizes a symbol buffer to ensure it can hold at least  symbols. The function adds a 20-symbol padding to the requested size for efficiency and uses  to resize the memory block. The buffer includes a header section (HEAD) that stores metadata such as the buffer's capacity. If memory reallocation fails, the function automatically frees the original buffer using  and returns NULL to indicate failure.

## Parameters / Member Variables
- : Pointer to the existing symbol buffer to be resized
- : Minimum number of symbols the buffer should accommodate

## Dependencies
- Functions called/Symbols referenced:
  - realloc
  - lose_s
  - HEAD (macro for header size)
  - CAPACITY (macro for accessing buffer capacity)
  - symbol (type definition)
- Called from (representative examples):
  - replace_s
  - slice_to
  - assign_to

## Notes and Other Information
- This is a static function, only accessible within the utilities.c file
- The function adds 20 extra symbols to the requested size for performance optimization
- Memory layout includes a header section before the actual symbol data
- On allocation failure, the original buffer is automatically freed to prevent memory leaks
- Part of PostgreSQL's Snowball stemming algorithm implementation for text processing