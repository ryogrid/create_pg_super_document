# stringify_grant_objtype

## Location
[src/backend/commands/event_trigger.c:2121-2205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L2121-L2205)

## Overview
Converts PostgreSQL ObjectType enumeration values to their corresponding string representations as they appear in GRANT and REVOKE commands.

## Definition
```c
static const char *stringify_grant_objtype(ObjectType objtype)
```

## Detailed Description
This static function serves as a mapping utility that translates PostgreSQL's internal ObjectType enumeration values into the standardized string representations used in SQL GRANT and REVOKE statements. The function is specifically designed to handle the subset of object types that support privilege operations in PostgreSQL.

The function includes explicit handling for object types that are supported in GRANT/REVOKE operations, such as TABLE, SEQUENCE, DATABASE, FUNCTION, SCHEMA, etc. It also contains a comprehensive list of object types that are currently not supported for privilege operations, which will trigger an error if encountered.

## Parameters / Member Variables
- `objtype`: The ObjectType enumeration value to be converted to its string representation

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - ObjectType enumeration constants (OBJECT_TABLE, OBJECT_FUNCTION, etc.)
- Called from (representative examples):
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md) (src/backend/commands/event_trigger.c:2097)

## Notes and Other Information
- Returns string constants that match the exact syntax used in SQL GRANT and REVOKE statements
- Includes special mappings for object types with multi-word names (e.g., "FOREIGN DATA WRAPPER", "LARGE OBJECT")
- Contains explicit error handling for unsupported object types, indicating that they cannot be used with GRANT/REVOKE
- The function distinguishes between FUNCTION, PROCEDURE, and ROUTINE object types to match SQL standard terminology
- Used primarily in event trigger contexts to provide human-readable object type information for privilege-related DDL commands
- The extensive list of unsupported object types serves as documentation of PostgreSQL's privilege system limitations and helps prevent runtime errors by failing early on unsupported operations

## Simplified Source

```c
// Simplified version of stringify_grant_objtype
static const char *stringify_grant_objtype(ObjectType objtype) {
    switch (objtype) {
        // Supported object types for GRANT/REVOKE operations
        case OBJECT_COLUMN:         return "COLUMN";
        case OBJECT_TABLE:          return "TABLE";
        case OBJECT_SEQUENCE:       return "SEQUENCE";
        case OBJECT_DATABASE:       return "DATABASE";
        case OBJECT_DOMAIN:         return "DOMAIN";
        case OBJECT_FDW:            return "FOREIGN DATA WRAPPER";
        case OBJECT_FOREIGN_SERVER: return "FOREIGN SERVER";
        case OBJECT_FUNCTION:       return "FUNCTION";
        case OBJECT_LANGUAGE:       return "LANGUAGE";
        case OBJECT_LARGEOBJECT:    return "LARGE OBJECT";
        case OBJECT_SCHEMA:         return "SCHEMA";
        case OBJECT_PARAMETER_ACL:  return "PARAMETER";
        case OBJECT_PROCEDURE:      return "PROCEDURE";
        case OBJECT_ROUTINE:        return "ROUTINE";
        case OBJECT_TABLESPACE:     return "TABLESPACE";
        case OBJECT_TYPE:           return "TYPE";

        // All other object types are not supported for GRANT/REVOKE
        default:
            elog(ERROR, "unsupported object type: %d", (int) objtype);
    }

    return "???";  // Never reached, keeps compiler happy
}
```

Key simplifications made:
- Consolidated all supported object types into clear switch cases
- Replaced the long list of unsupported cases with a default case
- Aligned the return statements for better readability
- Kept essential comments while removing redundant ones
- Maintained the error handling behavior for unsupported types