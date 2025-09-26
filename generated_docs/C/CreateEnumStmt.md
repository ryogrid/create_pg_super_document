# CreateEnumStmt

## Location
[src/include/nodes/parsenodes.h:3696-3701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3696-L3701)

## Overview
CreateEnumStmt represents a CREATE TYPE statement for defining enumeration types in PostgreSQL's parse tree structure.

## Definition
```c
typedef struct CreateEnumStmt
{
    NodeTag     type;
    List       *typeName;       /* qualified name (list of String) */
    List       *vals;           /* enum values (list of String) */
} CreateEnumStmt;
```

## Detailed Description
CreateEnumStmt is a parse tree node that represents CREATE TYPE statements used to define enumeration types in PostgreSQL. Enumeration types are data types that consist of a static, ordered set of values. They are useful for representing data that has a known, fixed set of possible values such as days of the week, months, status codes, or any other categorical data. The statement captures both the qualified name of the enum type and the list of possible values it can contain.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CreateEnumStmt node
- `typeName`: List of String nodes representing the qualified name of the enum type (e.g., schema.type_name)
- `vals`: List of String nodes containing the enumeration values in their defined order

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - List (containing String nodes)
- Called from (representative examples):
  - DefineEnum
  - ProcessUtilitySlow

## Notes and Other Information
- Enum values are ordered and the order is significant for comparison operations
- Once created, enum types can be used as column types in tables and as function parameters/return types
- Enum values must be unique within the enumeration
- PostgreSQL allows adding new values to existing enums using ALTER TYPE
- Enum types provide type safety and can help prevent invalid data entry
- The typeName list supports schema-qualified names for proper namespace resolution
- Enum values are stored as strings but are compared by their ordinal position for efficiency