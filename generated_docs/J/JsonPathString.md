# JsonPathString

## Location
[src/backend/utils/adt/jsonpath_internal.h:18-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_internal.h#L18-L23)

## Overview
A utility structure used by the PostgreSQL jsonpath lexical scanner and parser to efficiently manage dynamic string buffers during tokenization.

## Definition

```c
typedef struct JsonPathString
{
	char	   *val;
	int			len;
	int			total;
} JsonPathString;
```
## Detailed Description
JsonPathString is a fundamental data structure used internally by PostgreSQL's jsonpath implementation to handle dynamic string construction during the lexical analysis phase. This structure provides an efficient mechanism for building strings of unknown final length by maintaining both the current length and total allocated capacity, allowing for optimal memory management through exponential growth strategies.

The structure is specifically designed to be shared between the scanner (jsonpath_scan.l) and grammar (jsonpath_gram.y) components of the jsonpath parser. It serves as the primary vehicle for capturing and passing string tokens from the lexer to the parser, including string literals, variable names, numeric values, and other textual elements found in jsonpath expressions.

The implementation uses a dynamic buffer approach where the allocated space (total) can grow beyond the current string length (len), reducing the need for frequent memory reallocations during string construction. When additional space is needed, the buffer size is doubled using PostgreSQL's repalloc() function.

## Parameters / Member Variables
- : Pointer to the dynamically allocated character buffer containing the string data
- : Current length of the string stored in the buffer (excluding null terminator)
- : Total allocated size of the buffer, which may be larger than len to accommodate future growth

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for initial memory allocation)
  - [repalloc](../r/repalloc.md) (for buffer resizing)
  - memcpy (for string data copying)
- Called from (representative examples):
  - addstring (adds string data to JsonPathString)
  - addchar (adds single character to JsonPathString)
  - resizeString (manages buffer allocation and growth)
  - makeItemString (converts JsonPathString to JsonPathParseItem)
  - makeItemVariable (converts JsonPathString to variable parse item)
  - makeItemKey (converts JsonPathString to key parse item)
  - makeItemNumeric (converts JsonPathString to numeric parse item)

## Notes and Other Information
- This structure is used exclusively during jsonpath parsing and is not exposed to external code
- The buffer management follows an exponential growth strategy, starting with a minimum of 32 bytes and doubling when more space is needed
- String data stored in val does not necessarily include null terminators unless explicitly added via addchar()
- The structure supports efficient string concatenation operations without requiring knowledge of the final string length
- Memory allocated for the val field is managed through PostgreSQL's memory context system
- The structure definition is located in src/backend/utils/adt/jsonpath_internal.h:18-23