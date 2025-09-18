# filter_get_keyword

## Location
src/bin/pg_dump/filter.c: 180 - 217

## Overview
Reads the next filter keyword from a line buffer by extracting strings of non-whitespace characters.

## Definition


## Detailed Description
This function searches for keywords (strings of non-whitespace characters) in the passed line buffer. It skips any initial whitespace and then extracts the first sequence of non-whitespace characters as a keyword. The function returns a pointer to the start of the keyword in the original buffer, and updates the line pointer to point past the found keyword. If no keyword is found (buffer is empty or contains only whitespace), it returns NULL and sets the size to 0.

## Parameters / Member Variables
- : Pointer to a pointer to the current position in the line buffer (updated to point past the found keyword)
- : Pointer to an integer that will receive the length of the found keyword (set to 0 if no keyword found)

## Dependencies
- Functions called/Symbols referenced:
  - isspace (standard C library function)
  - Assert (PostgreSQL assertion macro)
- Called from (representative examples):
  - [filter_read_item](filter_read_item.md) (at src/bin/pg_dump/filter.c:421)
  - [filter_read_item](filter_read_item.md) (at src/bin/pg_dump/filter.c:440)

## Notes and Other Information
- This is a static function, only accessible within the filter.c file
- The function modifies the input line pointer to advance past the processed keyword
- Uses standard C library isspace() function with explicit unsigned char casting for proper handling of extended ASCII characters
- Part of pg_dump's filtering mechanism for selectively dumping database objects
- Returns a pointer directly into the original buffer rather than allocating new memory