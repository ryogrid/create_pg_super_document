# BitString

## Location
[src/include/nodes/value.h:71-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/value.h#L71-L77)

## Overview
The BitString node type represents bit string literal constants in PostgreSQL's parser, providing a node-based wrapper for binary string values.

## Definition
```c
typedef struct BitString
{
    pg_node_attr(special_read_write)

    NodeTag     type;
    char       *bsval;
} BitString;
```

## Detailed Description
The BitString node is part of PostgreSQL's value node family, designed to represent bit string literals that appear in SQL statements within the parser's abstract syntax tree. It serves as a container for binary string constants, allowing them to be treated as first-class nodes within the parsing system.

Bit strings in PostgreSQL are sequences of bits (0s and 1s) that can be specified using binary literal notation (e.g., B'1010') or hexadecimal notation (e.g., X'A'). The BitString node stores these values as character strings, preserving the original textual representation during the parsing phase.

Like other value nodes (Integer, Float, Boolean, String), the BitString node can be stored in PostgreSQL's List data structure and follows the standard node conventions for memory management and serialization.

## Parameters / Member Variables
- `type`: NodeTag that identifies this as a BitString node type within the node system
- `bsval`: Pointer to a null-terminated character string containing the textual representation of the bit string value

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node type identification)

- Called from (representative examples):
  - [makeBitString](../m/makeBitString.md) (creates BitString nodes)
  - [_outBitString](../o/_outBitString.md) (serializes BitString nodes)
  - [_readA_Const](../r/_readA_Const.md) (reads BitString nodes during deserialization)

## Notes and Other Information
- The BitString node includes the `pg_node_attr(special_read_write)` attribute for special serialization handling
- [BitString](BitString.md) nodes can be stored in List structures, unlike plain char* values
- The `strVal()` macro (defined in value.h:88) provides convenient access to the bsval field
- Memory for the bsval string is managed through PostgreSQL's memory context system
- Used primarily during parsing phase to represent bit string literals in SQL statements
- Part of the unified value node system that replaced the older Value node
- The textual representation preserves the original format of the bit string literal as it appeared in the SQL statement
- Less commonly used compared to other value node types, as bit string literals are not frequently used in typical SQL queries