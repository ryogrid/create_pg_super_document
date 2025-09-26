# FormatNode

## Location
[src/backend/utils/adt/formatting.c:159-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L159-L160)

## Overview
A structure representing a single node in PostgreSQL's parsed format template, used to store individual formatting elements during date/time and numeric formatting operations.

## Definition

```c
typedef struct
{
	uint8		type;			/* NODE_TYPE_XXX, see below */
	char		character[MAX_MULTIBYTE_CHAR_LEN + 1];	/* if type is CHAR */
	uint8		suffix;			/* keyword prefix/suffix code, if any */
	const KeyWord *key;			/* if type is ACTION */
} FormatNode;
```
## Detailed Description
FormatNode represents a single element in a parsed format string used by PostgreSQL's formatting functions like , , and . When a format template string is parsed, it is broken down into an array of FormatNode structures, each representing either a formatting directive, literal character, separator, or space.

The structure supports different node types:
- ACTION nodes contain formatting keywords (like 'YYYY', 'MM', 'DD') 
- CHAR nodes contain literal characters to be copied
- SEPARATOR and SPACE nodes represent formatting separators
- END nodes mark the end of the format array

This parsed representation allows efficient processing during formatting operations without re-parsing the format string each time.

## Parameters / Member Variables
- : Specifies the node type using NODE_TYPE_XXX constants (END=1, ACTION=2, CHAR=3, SEPARATOR=4, SPACE=5)
- : Stores literal character data when type is NODE_TYPE_CHAR, supporting multibyte characters up to MAX_MULTIBYTE_CHAR_LEN
- : Stores prefix/suffix code for keywords that support modifiers (SUFFTYPE_PREFIX=1, SUFFTYPE_POSTFIX=2)
- : Pointer to the KeyWord structure containing formatting directive details when type is NODE_TYPE_ACTION

## Dependencies
- Functions called/Symbols referenced:
  - KeyWord struct (for ACTION nodes)
  - NODE_TYPE_XXX constants
  - SUFFTYPE_XXX constants
- Called from (representative examples):
  - [parse_format](../p/parse_format.md): Creates and populates FormatNode arrays
  - [DCH_to_char](../D/DCH_to_char.md): Processes FormatNode array for date/time formatting
  - [DCH_from_char](../D/DCH_from_char.md): Processes FormatNode array for date/time parsing
  - [NUM_processor](../N/NUM_processor.md): Processes FormatNode array for numeric formatting
  - [dump_node](../d/dump_node.md): Debug function to display FormatNode contents

## Notes and Other Information
- [FormatNode](FormatNode.md) arrays are typically allocated as  to accommodate the END node
- The structure is used extensively in PostgreSQL's formatting system for both input parsing and output generation
- Caching mechanisms store parsed FormatNode arrays to avoid re-parsing frequently used format strings
- The character array supports full multibyte character sequences, enabling international character support in format templates
- Different processing functions (DCH_*, NUM_*) interpret the same FormatNode structure differently based on context