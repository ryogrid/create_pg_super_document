# DefineType

## Location
src/backend/commands/typecmds.c: 152 - 656

## Overview
DefineType registers a new base type in the PostgreSQL type system, handling both shell type creation and full type definition with all necessary I/O functions and properties.

## Definition


## Detailed Description
DefineType is the primary function for creating new base types in PostgreSQL. It operates in two modes:

1. **Shell Type Creation**: When called with no parameters, it creates a "shell" type entry that can later be filled in with the actual type definition.
2. **Full Type Definition**: When called with parameters, it expects a shell type to already exist and fills in the complete type definition.

The function requires superuser privileges and performs extensive validation of all type properties. It automatically creates a corresponding array type for the new base type and handles complex parameter parsing for all type attributes including I/O functions, storage properties, and type modifiers.

The function follows PostgreSQL's type creation protocol where I/O functions must be created before the type itself, necessitating the two-phase creation process with shell types.

## Parameters / Member Variables
- : ParseState context for error reporting and location tracking
- : List of qualified names specifying the type name and optional schema
- : List of DefElem structures containing type definition parameters (NULL for shell creation)

### Key Type Parameters (when creating full definition):
- : Copy properties from an existing type
- : Internal storage size (-1 for variable length)
- : Required I/O function names
- : Optional binary I/O functions
- : Type modifier functions
- : Statistics collection function
- : Array subscripting function
- : Type category character (default 'U' for user)
- : Whether type is preferred in its category
- : Array element delimiter character
- : Element type for array-like types
- : Default value expression
- : Whether values passed by value vs reference
- : Memory alignment requirement
- : TOAST storage strategy
- : Whether type supports collation

## Dependencies
- Functions called/Symbols referenced:
  - [TypeShellMake](../T/TypeShellMake.md): Creates shell type entries
  - [TypeCreate](../T/TypeCreate.md): Creates the actual type catalog entry
  - [moveArrayTypeName](../m/moveArrayTypeName.md): Handles array type naming conflicts
  - [makeArrayTypeName](../m/makeArrayTypeName.md): Generates array type names
  - [AssignTypeArrayOid](../A/AssignTypeArrayOid.md): Allocates OID for array type
  - [findTypeInputFunction](../f/findTypeInputFunction.md), findTypeOutputFunction: Locate I/O functions
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md): Parse qualified names
  - Various defGet* functions: Extract parameter values

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main DDL command processing

## Notes and Other Information
- Requires superuser privileges for security reasons due to the complexity of type creation
- Automatically creates a corresponding array type for every base type
- Uses a two-phase creation process: shell type first, then full definition
- Extensive parameter validation ensures type system integrity
- Array types use specialized I/O functions (array_in, array_out, etc.)
- Type OIDs must be preserved during binary upgrades
- The function handles backwards compatibility for various parameter synonyms
- Storage alignment is automatically adjusted for array types (INT or DOUBLE only)