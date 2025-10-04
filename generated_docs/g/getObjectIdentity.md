# getObjectIdentity

## Location
[src/backend/catalog/objectaddress.c:4740-4754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4740-L4754)

## Overview
A public function that obtains a human-readable string representation of a database object's identity, suitable for machine consumption and logging purposes.

## Definition
```c
char *getObjectIdentity(const ObjectAddress *object, bool missing_ok)
```

## Detailed Description
This function serves as a simplified interface to getObjectIdentityParts, providing a complete string representation of a database object's identity. The returned string is palloc'd and must be freed by the caller. All elements in the identity string are schema-qualified when appropriate, making it suitable for unambiguous object identification in logs, error messages, and system catalogs.

The function is designed for machine consumption rather than user display, so the output is not translated and follows a consistent format regardless of locale. If the specified object cannot be found and missing_ok is false, the function will raise an error; if missing_ok is true, it returns NULL.

## Parameters / Member Variables
- `object`: Pointer to an ObjectAddress structure containing the object's class ID, object ID, and sub-object ID
- `missing_ok`: Boolean flag indicating whether to handle missing objects gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [getObjectIdentityParts](getObjectIdentityParts.md) (core implementation function)

- Called from (representative examples):
  - [pg_identify_object](../p/pg_identify_object.md) (SQL function for object identification)
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md) (event trigger system)
  - ObjectAddressSet (object address utility macro/function)

## Notes and Other Information
- This is a wrapper function that delegates to getObjectIdentityParts with NULL parameters for object name and schema name output
- The returned string is palloc'd memory that must be freed by the caller
- Output format is consistent and not localized, making it suitable for machine processing
- Schema qualification is applied automatically when needed for unambiguous identification
- Returns NULL when the object is not found and missing_ok is true
- Part of PostgreSQL's object address and identification infrastructure used throughout the system

## Simplified Source

```c
char *
getObjectIdentity(const ObjectAddress *object, bool missing_ok)
{
    return getObjectIdentityParts(object, NULL, NULL, missing_ok);
}
```