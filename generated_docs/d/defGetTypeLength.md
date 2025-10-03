# defGetTypeLength

## Location
[src/backend/commands/define.c:312-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/define.c#L312-L355)

## Overview
Extracts a type length indicator from a DefElem, returning either absolute byte length or -1 for variable-length types.

## Definition

```c
int
defGetTypeLength(DefElem *def)
```
## Detailed Description
The  function is a specialized utility for extracting type length specifications in PostgreSQL's type definition system. It handles the parsing of length parameters for data types, supporting both fixed-length types (specified as integer byte counts) and variable-length types (specified using the keyword "variable").

The function processes several input formats:
1. **T_Integer**: Direct integer values representing the byte length
2. **T_Float**: Rejected with an error since type lengths must be integers
3. **T_String**: Accepts the string "variable" (case-insensitive) and returns -1
4. **T_TypeName**: Handles cases where the grammar interprets "variable" as a type name
5. **T_List**: Operator names are not valid for type lengths

The function includes comprehensive error handling and provides detailed error messages for invalid inputs.

## Parameters / Member Variables
- `*def`: Pointer to a DefElem structure containing the definition element to extract the type length from
## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (macro to get node type)
  - intVal (macro to extract integer value)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - strVal (macro to extract string value)
  - [TypeNameToString](../T/TypeNameToString.md) (function to convert TypeName to string)
  - [TypeName](../T/TypeName.md) (structure type)
  - [defGetString](defGetString.md) (function to get string representation)
- Called from (representative examples):
  - [DefineType](../D/DefineType.md) (type definition commands)
  - Functions declared in defrem.h

## Notes and Other Information
- Returns -1 to indicate variable-length types, following PostgreSQL's internal convention
- Provides case-insensitive matching for the "variable" keyword
- Handles grammar ambiguities where "variable" might be parsed as a type name
- Essential for CREATE TYPE statements that specify storage length
- Located in src/backend/commands/define.c:312-355
- Includes fallback return 0 to keep compiler quiet, though unreachable due to error handling

## Simplified Source

```c
int
defGetTypeLength(DefElem *def)
{
    // Ensure parameter is provided
    if (def->arg == NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                       errmsg("%s requires a parameter", def->defname)));

    // Process different argument types
    switch (nodeTag(def->arg)) {
        case T_Integer:
            return intVal(def->arg);  // Direct integer byte length

        case T_Float:
            // Reject float values - type lengths must be integers
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                           errmsg("%s requires an integer value", def->defname)));
            break;

        case T_String:
            // Check for "variable" keyword (case-insensitive)
            if (pg_strcasecmp(strVal(def->arg), "variable") == 0)
                return -1;  // Variable length indicator
            break;

        case T_TypeName:
            // Handle grammar ambiguity where "variable" parsed as typename
            if (pg_strcasecmp(TypeNameToString((TypeName *) def->arg), "variable") == 0)
                return -1;  // Variable length indicator
            break;

        case T_List:
            // Operator names not valid for type lengths
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(def->arg));
    }

    // Invalid argument - report error with context
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                   errmsg("invalid argument for %s: \"%s\"",
                          def->defname, defGetString(def))));
    return 0;  // Keep compiler quiet
}
```