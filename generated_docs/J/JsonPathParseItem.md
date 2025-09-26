# JsonPathParseItem

## Location
[src/include/utils/jsonpath.h:215-217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonpath.h#L215-L217)

## Overview
JsonPathParseItem is a data structure used during JSON path expression parsing to represent individual items or nodes in the parse tree for JSON path queries.

## Definition

```c
typedef struct JsonPathParseItem JsonPathParseItem;
```
## Detailed Description
JsonPathParseItem is a versatile structure that serves as the fundamental building block for representing parsed JSON path expressions. It uses a union to efficiently store different types of path items, from simple scalars to complex operators and array indexing operations. The structure forms a linked list through the 'next' pointer, allowing representation of sequential path operations. This design supports the full range of JSON path functionality including filter expressions, array slicing, regular expression matching, and logical operations.

## Parameters / Member Variables
- : JsonPathItemType enum value indicating the specific type of path item
- : Pointer to the next JsonPathParseItem in the path sequence
- : Union containing type-specific data:
  - : For binary operators (and, or, etc.) - contains left and right operands
  - : For unary operations - contains single operand
  - : For array indexing - contains number of elements and from/to ranges
  - : For recursive descent operations - contains first and last level bounds
  - : For regex pattern matching - contains expression, pattern, pattern length, and flags
  - : For numeric literal values
  - : For boolean literal values
  - : For string literals - contains length and value pointer (may not be null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - JsonPathItemType (enum for type identification)
- Called from (representative examples):
  - [flattenJsonPathParseItem](../f/flattenJsonPathParseItem.md)
  - [JsonPathParseResult](JsonPathParseResult.md) (as expr member)

## Notes and Other Information
- Part of PostgreSQL's JSON path parsing infrastructure
- The string values in the union may not be null-terminated, requiring length-based operations
- Forms the intermediate representation during JSON path compilation before conversion to the final JsonPath binary format
- The structure supports recursive parsing through self-referential pointers in various union members