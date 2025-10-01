# JsonbTypeName

## Location
[src/backend/utils/adt/jsonb.c:176-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L176-L220)

## Overview
Returns a human-readable string representation of the type name for a JsonbValue.

## Definition
```c
const char *
JsonbTypeName(JsonbValue *val)
```

## Detailed Description
This function provides a string representation of the type for any JsonbValue structure. It handles all possible JsonbValue types including basic types (object, array, number, string, boolean, null), datetime types with their specific subtypes, and binary containers. For binary containers, it delegates to JsonbContainerTypeName to determine the specific container type.

## Parameters / Member Variables
- `val`: Pointer to the JsonbValue whose type name is requested

## Dependencies
- Functions called/Symbols referenced:
  - JsonbContainerTypeName (for binary container type names)
  - elog (PostgreSQL error logging function)

## Notes and Other Information
- Provides human-readable type names for all JsonbValue types
- Handles datetime subtypes with specific time zone information
- Returns "unknown" for unrecognized types before throwing an error

## Simplified Source

```c
const char *
JsonbTypeName(JsonbValue *val)
{
    switch (val->type) {
        case jbvBinary:
            return JsonbContainerTypeName(val->val.binary.data);
        case jbvObject:
            return "object";
        case jbvArray:
            return "array";
        case jbvNumeric:
            return "number";
        case jbvString:
            return "string";
        case jbvBool:
            return "boolean";
        case jbvNull:
            return "null";
        case jbvDatetime:
            // Return specific datetime type name based on typid
            switch (val->val.datetime.typid) {
                case DATEOID: return "date";
                case TIMEOID: return "time without time zone";
                case TIMETZOID: return "time with time zone";
                case TIMESTAMPOID: return "timestamp without time zone";
                case TIMESTAMPTZOID: return "timestamp with time zone";
                default:
                    elog(ERROR, "unrecognized jsonb value datetime type: %d",
                         val->val.datetime.typid);
            }
            return "unknown";
        default:
            elog(ERROR, "unrecognized jsonb value type: %d", val->type);
            return "unknown";
    }
}
```