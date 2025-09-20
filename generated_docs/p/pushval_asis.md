# pushval_asis

## Location
[src/backend/utils/adt/tsquery.c:942-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L942-L951)

## Overview
The  function is a simple callback wrapper that pushes query operands directly to the TSQuery parser state without any transformation or processing.

## Definition

```c
typedef struct
{
	QueryItem  *curpol;
	char	   *buf;
	char	   *cur;
	char	   *op;
	int			buflen;
} INFIX;
```
## Detailed Description
This function serves as a straightforward PushFunction callback for the  function. It acts as a pass-through mechanism that takes parsed query operands and directly adds them to the query structure using  without any modification, normalization, or additional processing. The "asis" in the name indicates that values are pushed "as-is" without transformation.

This function is typically used when the caller wants the raw parsed operands to be included in the final TSQuery exactly as they appear in the input string, without any dictionary lookup, stemming, or other text processing that might be applied by more sophisticated push functions.

## Parameters / Member Variables
- : Unused opaque data parameter (required by PushFunction interface)
- : TSQuery parser state containing the current parsing context
- : The string value of the operand to be added
- : Length of the string value in bytes
- : Weight flags for the operand (A, B, C, D weights)
- : Boolean indicating if this is a prefix search operand

## Dependencies
- Functions called/Symbols referenced:
  - [pushValue](pushValue.md)
- Called from (representative examples):
  - [tsqueryin](../t/tsqueryin.md)

## Notes and Other Information
- This is a static function, only accessible within the tsquery.c module
- The  parameter is ignored since no additional state is needed for this simple pass-through operation
- This function provides the simplest possible implementation of the PushFunction callback interface
- Used primarily by  for direct string-to-TSQuery conversion without text search configuration processing