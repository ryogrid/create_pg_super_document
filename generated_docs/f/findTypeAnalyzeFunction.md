# findTypeAnalyzeFunction

## Location
[src/backend/commands/typecmds.c:2208-2234](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L2208-L2234)

## Overview
This function validates and retrieves the OID of a user-specified analyze function for a PostgreSQL data type, ensuring it meets the required signature constraints for type analysis operations.

## Definition


## Detailed Description
The  is a static helper function used during type definition and modification operations in PostgreSQL. It validates that a specified function exists and conforms to the required signature for type analyze functions. Type analyze functions are used by the PostgreSQL query planner to gather statistics about user-defined types, which helps in query optimization.

The function performs two key validations:
1. Ensures the specified function exists and takes exactly one INTERNAL argument
2. Verifies that the function returns a boolean value

If either validation fails, the function raises an appropriate error. This strict validation ensures that only properly formed analyze functions can be associated with custom types.

## Parameters / Member Variables
- : A List containing the qualified name components of the analyze function to validate
- : The OID of the type for which this analyze function is being set (currently unused but available for future enhancements)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupFuncName](../L/LookupFuncName.md): Locates the function by name and signature
  - [func_signature_string](func_signature_string.md): Formats function signature for error messages
  - [get_func_rettype](../g/get_func_rettype.md): Retrieves the return type of a function
  - [NameListToString](../N/NameListToString.md): Converts qualified name list to string representation
- Called from:
  - [DefineType](../D/DefineType.md): During creation of new user-defined types
  - [AlterType](../A/AlterType.md): When modifying existing type properties
  - AlterTypeRecurseParams: As part of recursive type alteration operations

## Notes and Other Information
- Type analyze functions must have the signature 
- The INTERNAL argument represents the type's internal storage format for analysis
- This function is part of PostgreSQL's extensible type system, allowing users to provide custom statistics gathering for their types
- The function is located in src/backend/commands/typecmds.c:2208-2234