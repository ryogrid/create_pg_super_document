# DefElemAction

## Location
[src/include/nodes/parsenodes.h:809-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L809-L810)

## Overview
DefElemAction is an enumeration that specifies the action type for definition elements in PostgreSQL SQL commands, supporting SET, ADD, DROP operations on configuration options and parameters.

## Definition


## Detailed Description
DefElemAction provides action semantics for DefElem nodes, which represent generic "name = value" option definitions in PostgreSQL SQL statements. This enum allows the parser to distinguish between different modification operations that can be performed on configuration parameters, table options, and other named settings. The actions correspond to SQL syntax patterns where options can be set to new values, added to existing lists, or removed entirely. This is particularly useful in commands like ALTER TABLE, CREATE INDEX, and other DDL statements that allow incremental modification of object properties.

## Parameters / Member Variables
- : No specific action specified, typically used for initial setting or simple assignment
- : Set or update the option to a new value (replaces existing value)
- : Add the option or add to an existing list of values
- : Remove or drop the specified option

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [DefElem](DefElem.md) (in defaction field)
  - makeDefElemExtended

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:803-809
- Used in conjunction with DefElem struct for option management in SQL commands
- The grammar restricts namespace and action usage to contexts where they are semantically relevant
- Essential for ALTER commands that need to modify existing object configurations incrementally
- Part of PostgreSQL's extensible option system that supports various SQL standard and extension-specific parameters