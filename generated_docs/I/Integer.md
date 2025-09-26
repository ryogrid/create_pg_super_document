# Integer

## Location
src/include/nodes/value.h: 28 - 34

## Overview
The Integer node type represents integer literal constants in PostgreSQL's parser and lexer, providing a way to encapsulate integer values within the node tree structure.

## Definition


## Detailed Description
The Integer node is one of the fundamental value node types in PostgreSQL's parse tree system. It serves as a container for integer literal values that need to be represented as nodes within the parser's abstract syntax tree. Unlike plain integer types, Integer nodes can be stored in PostgreSQL's List data structure, making them suitable for use in various parser contexts where collections of values are needed.

The Integer node type is part of a family of value nodes (Integer, Float, String, BitString) that replaced the older unified Value node type. These nodes are primarily used during lexical analysis and parsing to represent literal constants found in SQL statements.

## Parameters / Member Variables
- : NodeTag that identifies this as an Integer node type for the node system
- : The actual integer value stored in this node

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node type identification)
  
- Called from (representative examples):
  - makeInteger (creates Integer nodes)
  - _outInteger (serializes Integer nodes)
  - _readA_Const (reads Integer nodes during deserialization)
  - createdb (uses Integer nodes in database creation)
  - typenameTypeMod (uses Integer nodes for type modifiers)

## Notes and Other Information
- The Integer node includes the  attribute, indicating it requires special handling during node serialization/deserialization
- Integer nodes can be stored in List structures, unlike plain int values
- The  macro (defined in value.h:79) provides convenient access to the ival field
- This node type is part of PostgreSQL's node system and follows the standard node conventions for memory management and traversal