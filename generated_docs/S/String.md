# String

## Location
src/include/nodes/value.h: 63 - 69

## Overview
The String node type represents string literal constants in PostgreSQL's parser, providing a node-based wrapper for character string values.

## Definition
```c
typedef struct String
{
    pg_node_attr(special_read_write)

    NodeTag     type;
    char       *sval;
} String;
```

## Detailed Description
The String node is a fundamental component of PostgreSQL's value node system, designed to represent string literals that appear in SQL statements within the parser's abstract syntax tree. It serves as a container for character string constants, allowing them to be treated as first-class nodes within the parsing and analysis phases.

The String node is part of the value node family (Integer, Float, Boolean, String, BitString) that replaced the older unified Value node type. Like other value nodes, String nodes can be stored in PostgreSQL's List data structure, making them suitable for use in various parser contexts where collections of values are needed.

String nodes are extensively used throughout PostgreSQL's parsing and analysis systems, particularly in contexts involving identifiers, literals, and various SQL constructs that require string values.

## Parameters / Member Variables
- `type`: NodeTag that identifies this as a String node type within the node system
- `sval`: Pointer to a null-terminated character string containing the actual string value

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node type identification)

- Called from (representative examples):
  - makeString (creates String nodes)
  - _outString (serializes String nodes)
  - _readA_Const (reads String nodes during deserialization)
  - transformRangeTableFunc (uses String nodes in table function parsing)
  - FigureColnameInternal (uses String nodes for column name resolution)
  - get_rule_expr (uses String nodes in rule expression formatting)
  - typenameTypeMod (uses String nodes for type modifiers)

## Notes and Other Information
- The String node includes the `pg_node_attr(special_read_write)` attribute for special serialization handling
- String nodes can be stored in List structures, unlike plain char* values
- The `strVal()` macro (defined in value.h:82,87) provides convenient access to the sval field
- Memory for the sval string is managed through PostgreSQL's memory context system
- Widely used throughout the parser for representing identifiers, string literals, and various textual constructs
- This node type is essential for SQL parsing where string values need to be preserved and manipulated within the node tree structure
- Used extensively in query transformation, rule processing, and various SQL analysis operations