# Boolean

## Location
[src/include/nodes/value.h:55-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/value.h#L55-L61)

## Overview
The Boolean node type represents boolean literal constants (true/false) in PostgreSQL's parser, providing a node-based wrapper for boolean values.

## Definition
```c
typedef struct Boolean
{
    pg_node_attr(special_read_write)

    NodeTag     type;
    bool        boolval;
} Boolean;
```

## Detailed Description
The Boolean node is part of PostgreSQL's value node family, designed to represent boolean literal values within the parser's abstract syntax tree. It serves as a container for boolean constants that appear in SQL statements, allowing them to be treated as first-class nodes within the parsing system.

Like other value nodes (Integer, Float, String, BitString), the Boolean node can be stored in PostgreSQL's List data structure, making it suitable for use in contexts where collections of values are needed. The Boolean node is primarily used during lexical analysis and parsing phases to represent true/false literals found in SQL statements.

The Boolean node follows PostgreSQL's standard node conventions and includes the special read/write attribute for proper serialization handling.

## Parameters / Member Variables
- `type`: NodeTag that identifies this as a Boolean node type within the node system
- `boolval`: The actual boolean value (true or false) stored in this node

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node type identification)

- Called from (representative examples):
  - [makeBoolean](../m/makeBoolean.md) (creates Boolean nodes)
  - [_outBoolean](../o/_outBoolean.md) (serializes Boolean nodes)
  - [_readA_Const](../r/_readA_Const.md) (reads Boolean nodes during deserialization)

## Notes and Other Information
- The Boolean node includes the `pg_node_attr(special_read_write)` attribute for special serialization handling
- [Boolean](Boolean.md) nodes can be stored in List structures, unlike plain bool values
- The `boolVal()` macro (defined in value.h:81) provides convenient access to the boolval field
- This node type is part of PostgreSQL's unified value node system that replaced the older Value node
- Memory management follows PostgreSQL's memory context system
- Used primarily during parsing phase for representing boolean literals in SQL statements