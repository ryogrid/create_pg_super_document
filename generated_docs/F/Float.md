# Float

## Location
[src/include/nodes/value.h:47-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/value.h#L47-L53)

## Overview
The Float node type represents floating-point and large integer literal constants in PostgreSQL's parser, storing numeric values as strings to preserve precision during parsing.

## Definition
```c
typedef struct Float
{
    pg_node_attr(special_read_write)

    NodeTag     type;
    char       *fval;
} Float;
```

## Detailed Description
The Float node is a crucial component of PostgreSQL's value node system, designed to handle numeric literals that appear in SQL statements. Unlike typical floating-point representations, the Float node stores numeric values as character strings to preserve precision throughout the parsing process.

This design choice is particularly important because PostgreSQL may ultimately convert these values to NUMERIC type, which requires arbitrary precision. Storing as a C double would risk precision loss. Additionally, integer values that are too large to fit in a standard 'int' type are lexed as T_Float nodes rather than Integer nodes.

The Float node, like other value nodes (Integer, String, BitString), can be stored in List structures and is primarily used during the parsing phase rather than runtime execution.

## Parameters / Member Variables
- `type`: NodeTag that identifies this as a Float node type within the node system
- `fval`: Pointer to a null-terminated string containing the textual representation of the numeric value

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node type identification)

- Called from (representative examples):
  - [makeFloat](../m/makeFloat.md) (creates Float nodes)
  - [_outFloat](../o/_outFloat.md) (serializes Float nodes)
  - [_readA_Const](../r/_readA_Const.md) (reads Float nodes during deserialization)
  - [defGetInt64](../d/defGetInt64.md) (extracts integer values from Float nodes)
  - [typenameTypeMod](../t/typenameTypeMod.md) (uses Float nodes for type modifiers)
  - [oidparse](../o/oidparse.md) (parses OID values from Float nodes)

## Notes and Other Information
- [Float](Float.md) nodes use string representation internally to avoid precision loss during conversion to NUMERIC
- [Integer](../I/Integer.md) values too large for 'int' type are automatically lexed as Float nodes instead of Integer nodes
- The `floatVal()` macro (defined in value.h:80) provides convenient access to the fval field
- Includes `pg_node_attr(special_read_write)` attribute for special serialization handling
- Memory for the fval string is managed through PostgreSQL's memory context system
- Used primarily during parsing phase, not for runtime numeric operations