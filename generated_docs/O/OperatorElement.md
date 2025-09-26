# OperatorElement

## Location
[src/backend/utils/adt/tsquery.c:629-633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L629-L633)

## Overview
OperatorElement is a compact structure that represents an operator with its associated distance parameter in tsquery parsing operations, used primarily for managing operator precedence and proximity operations.

## Definition
```c
typedef struct OperatorElement
{
    int8        op;
    int16       distance;
} OperatorElement;
```

## Detailed Description
OperatorElement serves as a fundamental building block in PostgreSQL's tsquery parsing system for representing operators during expression parsing. This lightweight structure encapsulates two critical pieces of information: the operator type and its associated distance parameter. The structure is designed to be efficiently stored and manipulated on operator stacks during the parsing process, particularly when converting infix tsquery expressions to Polish notation. The distance field is especially important for proximity operators where spatial relationships between terms need to be preserved and evaluated.

## Parameters / Member Variables
- `op`: 8-bit integer representing the operator type (e.g., AND, OR, NOT, proximity operators)
- `distance`: 16-bit integer specifying the distance parameter for proximity operations or other operator-specific numeric values

## Dependencies
- Functions called/Symbols referenced:
  - int8 (PostgreSQL integer type)
- Called from (representative examples):
  - [pushOpStack](../p/pushOpStack.md)
  - [cleanOpStack](../c/cleanOpStack.md)  
  - [makepol](../m/makepol.md)

## Notes and Other Information
The compact design of OperatorElement (only 3 bytes total) makes it efficient for stack operations during parsing. The structure is primarily used in conjunction with operator stack management functions to handle operator precedence and associativity rules during tsquery parsing. The distance field supports PostgreSQL's proximity search capabilities, allowing users to specify how close terms should be to each other in the document. This structure is typically instantiated temporarily during parsing and does not persist beyond the parsing phase.