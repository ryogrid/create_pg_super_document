# mbinterval

## Location
[src/common/wchar.c:573-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L573-L580)

## Overview
A structure that defines a Unicode character range interval used for determining character display width properties in PostgreSQL's wide character handling.

## Definition

```c
struct mbinterval
{
	unsigned int first;
	unsigned int last;
};
```
## Detailed Description
The  structure represents a contiguous range of Unicode code points, used primarily in PostgreSQL's implementation of  functionality for determining the display width of wide characters. This structure is part of the wide character handling system that implements the Single UNIX Specification for character width calculation.

The structure is used to define intervals in lookup tables for different character categories such as:
- Non-spacing characters (zero-width characters)
- East Asian wide/fullwidth characters (double-width characters)

These intervals are used in binary search operations to efficiently determine whether a given Unicode code point falls within a specific character category, which then determines its display width (0, 1, or 2 columns).

## Parameters / Member Variables
- : The first Unicode code point in the interval (inclusive)
- 
wtmp begins Sun Aug 20 19:22:10 2023: The last Unicode code point in the interval (inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a data structure)
- Called from (representative examples):
  -  (used as array element type in binary search)
  -  (indirectly through mbbisearch calls)

## Notes and Other Information
- Originally based on Markus Kuhn's wcwidth implementation (2001-09-08, public domain)
- Used in static lookup tables defined in generated header files:
  -  - for zero-width characters
  -  - for double-width characters
- The intervals are used with the  function to perform efficient binary searches
- Critical for proper text rendering and cursor positioning in PostgreSQL's terminal interfaces
- Supports the full Unicode range up to U+0010FFFF