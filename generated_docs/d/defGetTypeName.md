# defGetTypeName

## Location
[src/backend/commands/define.c:284-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/define.c#L284-L311)

## Overview
Extracts a TypeName structure from a DefElem, specifically designed for DDL commands that require type specifications.

## Definition

```c
TypeName *
defGetTypeName(DefElem *def)
```
## Detailed Description
The  function is a specialized utility that extracts TypeName structures from DefElem nodes in PostgreSQL's DDL command processing. Unlike , this function specifically returns a TypeName structure, which contains additional type-specific information beyond just the name components.

The function handles two primary cases:
1. **T_TypeName**: Returns the TypeName structure directly when already in the correct format
2. **T_String**: Converts a quoted string into a TypeName structure for backward compatibility

An important design note: this function deliberately does not accept List arguments, because the parser only returns bare Lists when the name resembles an operator name, which is not appropriate for type names.

## Parameters / Member Variables
- `*def`: Pointer to a DefElem structure containing the definition element to extract the type name from
## Dependencies
- Functions called/Symbols referenced:
  - [DefElem](../D/DefElem.md) (structure type)
  - nodeTag (macro to get node type)
  - [TypeName](../T/TypeName.md) (structure type for type names)
  - [makeTypeNameFromNameList](../m/makeTypeNameFromNameList.md) (function to create TypeName from name list)
  - list_make1 (function to create single-element list)
- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md) (aggregate definition commands)
  - [DefineOperator](../D/DefineOperator.md) (operator definition commands)
  - [DefineType](../D/DefineType.md) (type definition commands)
  - [init_params](../i/init_params.md) (sequence parameter initialization)

## Notes and Other Information
- Returns NULL as a fallback to keep the compiler quiet, though this should never be reached due to error handling
- Specifically designed for type name extraction, providing more structured type information than defGetQualifiedName
- Does not accept List arguments by design, unlike defGetQualifiedName
- Provides backward compatibility support for quoted type names
- Essential for DDL commands that work with data types
- Located in src/backend/commands/define.c:284-311

## Simplified Source

```c
TypeName *
defGetTypeName(DefElem *def)
{
    // Validate that parameter is provided
    if (def->arg == NULL)
        ereport(ERROR, "%s requires a parameter", def->defname);

    // Handle different argument types
    switch (nodeTag(def->arg)) {
        case T_TypeName:
            // Already in correct format, return directly
            return (TypeName *) def->arg;

        case T_String:
            // Convert quoted string to TypeName for backwards compatibility
            return makeTypeNameFromNameList(list_make1(def->arg));

        default:
            // Invalid argument type
            ereport(ERROR, "argument of %s must be a type name", def->defname);
    }

    return NULL;  // Keep compiler quiet
}
```