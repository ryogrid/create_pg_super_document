# read_objtype_from_string

## Location
[src/backend/catalog/objectaddress.c:2600-2619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2600-L2619)

## Overview
Converts a string representation of an object type into PostgreSQL's internal ObjectType enumeration value, providing validation and error handling for unrecognized types.

## Definition


## Detailed Description
The `read_objtype_from_string` function serves as a string-to-enum converter for PostgreSQL's object type system. It takes a human-readable object type name (such as "table", "function", "index", etc.) and returns the corresponding ObjectType enumeration value used internally by PostgreSQL.

The function performs a linear search through the ObjectTypeMap array, which contains mappings between string names (tm_name) and their corresponding ObjectType enumeration values (tm_type). This mapping table must be kept in sync with the getObjectTypeDescription function to ensure consistency between string representations and internal types.

When an unrecognized object type string is provided, the function reports an error with a clear message indicating the invalid parameter. The function is designed to be strict about input validation, ensuring that only valid, supported object types are accepted.

## Parameters / Member Variables
- `objtype`: Null-terminated string containing the object type name to convert (e.g., "table", "function", "operator")

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length calculation)
  - strcmp (string comparison)
  - ereport (error reporting)
  - ObjectTypeMap (static mapping table)
- Called from (representative examples):
  - [pg_get_object_address](../p/pg_get_object_address.md) (src/backend/catalog/objectaddress.c:2119)
  - ObjectAddressSet (src/include/catalog/objectaddress.h:81)

## Notes and Other Information
- Returns the ObjectType enumeration value on success, or reports an error for invalid input
- The ObjectTypeMap must be kept synchronized with getObjectTypeDescription function
- Performs case-sensitive string matching against known object type names
- Used primarily by SQL-callable functions that accept object type strings as parameters
- The function includes a "keep compiler quiet" return statement that should never be reached due to the error reporting
- Essential for bridging between user-facing string representations and internal PostgreSQL object type handling