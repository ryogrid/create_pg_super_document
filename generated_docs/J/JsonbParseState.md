# JsonbParseState

## Location
src/include/utils/jsonb.h: 319 - 326

## Overview
JsonbParseState maintains conversion state during JSONB parsing from text format or type coercion operations, implementing a stack-based parser state machine.

## Definition
```c
typedef struct JsonbParseState
{
    JsonbValue              contVal;
    Size                    size;
    struct JsonbParseState *next;
    bool                    unique_keys;    /* Check object key uniqueness */
    bool                    skip_nulls;     /* Skip null object fields */
} JsonbParseState;
```

## Detailed Description
JsonbParseState forms a linked stack structure that tracks the parsing state when converting text JSON to binary JSONB format or during type coercion operations. Each state represents a level in the JSON hierarchy (objects and arrays), with the stack growing as the parser enters nested structures and shrinking as it exits them.

The structure maintains the current container being built (contVal), tracks memory usage (size), and provides configuration options for parsing behavior such as key uniqueness validation and null field handling. The linked list design allows the parser to maintain context for nested JSON structures while building the final JSONB representation.

## Parameters / Member Variables
- `contVal`: JsonbValue representing the current container (array or object) being constructed at this parser level
- `size`: Size tracking for memory management during the parsing process
- `next`: Pointer to the next JsonbParseState in the stack, representing the parent container level
- `unique_keys`: Boolean flag indicating whether to enforce object key uniqueness during parsing
- `skip_nulls`: Boolean flag indicating whether to skip null-valued object fields during parsing

## Dependencies
- Functions called/Symbols referenced:
  - JsonbValue (for contVal member)
  - Size (PostgreSQL size type)
  - struct JsonbParseState (self-reference for linked list)
- Called from (representative examples):
  - JsonbValueToJsonb
  - pushJsonbValue
  - pushJsonbValueScalar
  - pushState
  - appendKey
  - appendValue
  - appendElement
  - jsonb_set_element
  - jsonb_concat
  - setPath

## Notes and Other Information
The structure implements a stack-based parsing approach where each level corresponds to a JSON container (array or object). The unique_keys and skip_nulls flags provide parsing configuration options that affect how the final JSONB is constructed. Memory management is tracked through the size field to ensure efficient allocation during the parsing process. The linked list design allows arbitrary nesting depth limited only by available memory.