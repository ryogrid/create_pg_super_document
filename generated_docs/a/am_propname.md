# am_propname

## Location
[src/backend/utils/adt/amutils.c:25-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L25-L89)

## Overview
A simple structure that maps string property names to their corresponding IndexAMProperty enum values for efficient property lookup in index access method operations.

## Definition

```c
struct am_propname
{
	const char *name;
	IndexAMProperty prop;
};
```
## Detailed Description
The  structure serves as a mapping table element that converts human-readable string property names (like "asc", "desc", "orderable") into their corresponding IndexAMProperty enum values. This structure is used internally by PostgreSQL's index access method utility functions to provide efficient string-to-enum conversion when processing index property queries.

The structure is primarily used in the static array  defined in the same file, which contains predefined mappings for all standard index access method properties. This design pattern enables fast lookup of property enums based on string names provided by SQL functions like , , and .

## Parameters / Member Variables
- : A constant string containing the human-readable property name (e.g., "asc", "desc", "orderable", "clusterable")
- : The corresponding IndexAMProperty enum value (e.g., AMPROP_ASC, AMPROP_DESC, AMPROP_ORDERABLE, AMPROP_CLUSTERABLE)

## Dependencies
- Functions called/Symbols referenced:
  - IndexAMProperty (enum type)
- Used by:
  - am_propnames[] (static array)
  - [lookup_prop_name](../l/lookup_prop_name.md)() function (indirectly through am_propnames array)

## Notes and Other Information
- The structure is defined in 
- It is used exclusively within the amutils.c file as part of the SQL-level API for index access methods
- The static array  contains 18 predefined property mappings covering all standard PostgreSQL index properties
- Property names are case-insensitive when looked up (using )
- If a property name is not found in the predefined mappings,  is returned, allowing index access methods to define their own custom properties
- This design supports both core PostgreSQL index properties and extension-defined properties in a unified interface