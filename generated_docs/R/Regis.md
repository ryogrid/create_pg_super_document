# Regis

## Location
[src/include/tsearch/dicts/regis.h:32-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/dicts/regis.h#L32-L39)

## Overview
Regis is the main structure that represents a compiled fast regular expression subset pattern used by PostgreSQL's ISpell dictionary implementation for efficient text search pattern matching.

## Definition

```c
typedef struct Regis
{
	RegisNode  *node;
	uint32
				issuffix:1,
				nchar:16,
				unused:15;
} Regis;
```
## Detailed Description
Regis serves as the top-level container for a compiled regular expression pattern in PostgreSQL's fast regex subset engine. This structure is specifically designed for ISpell dictionaries and provides an efficient way to store and execute pattern matching operations. The structure contains a pointer to a linked list of RegisNode elements that represent the compiled pattern, along with metadata about the pattern's characteristics. The design emphasizes memory efficiency through bitfield packing while maintaining the necessary functionality for fast pattern matching in text search operations. The regex subset supported includes basic character matching and character class operations ([abc] and [^abc] patterns).

## Parameters / Member Variables
- : Pointer to the first RegisNode in the linked list representing the compiled regex pattern
- : 1-bit flag indicating whether this pattern is used for suffix matching (1) or prefix matching (0)
- : 16-bit field storing the number of characters in the compiled pattern
- : 15-bit padding field reserved for future use or alignment purposes

## Dependencies
- Functions called/Symbols referenced:
  - RegisNode (linked list nodes containing pattern data)
- Called from (representative examples):
  - RS_compile (compiles string patterns into Regis structures)
  - RS_free (releases memory allocated for Regis structures)
  - RS_execute (executes pattern matching against input strings)
  - aff_struct (used in spell.h for affix processing in ISpell)

## Notes and Other Information
- Part of PostgreSQL's text search infrastructure, specifically the ISpell dictionary implementation
- Supports a limited but efficient subset of regular expression operations optimized for dictionary pattern matching
- The regex subset includes: alphabetic characters, character classes [abc], and negated character classes [^abc]
- The issuffix flag allows the same structure to handle both prefix and suffix pattern matching efficiently
- Memory layout is optimized using bitfields to minimize space overhead while maintaining functionality
- Used in conjunction with RS_isRegis(), RS_compile(), RS_free(), and RS_execute() functions for complete regex subset functionality