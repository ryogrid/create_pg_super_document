# findTypeReceiveFunction

## Location
src/backend/commands/typecmds.c: 2051 - 2104

## Overview
Validates and retrieves the OID of a type's receive function, which converts external binary representation to internal format, supporting both single-argument and three-argument function signatures.

## Definition
```c
static Oid findTypeReceiveFunction(List *procname, Oid typeOid)
```

## Detailed Description
This function locates and validates a type receive function during type definition or modification. Type receive functions are essential components that convert PostgreSQL's external binary representation of a data type to its internal binary format. The function supports two distinct signatures: a simple single-argument version taking only INTERNAL, and a more complex three-argument version that also accepts typioparam OID and typmod parameters. The function performs ambiguity checking to ensure only one form exists and validates that the function returns the correct type.

## Parameters / Member Variables
- `procname`: A list representing the qualified name of the receive function to look up
- `typeOid`: The OID of the data type for which this will serve as the receive function

## Dependencies
- Functions called/Symbols referenced:
  - LookupFuncName: Looks up function by name with specified argument types (called twice for different signatures)
  - NameListToString: Converts a name list to string format for display
  - func_signature_string: Creates a string representation of function signature for error messages
  - get_func_rettype: Retrieves the return type OID of a function
  - format_type_be: Formats a type OID as a readable type name
  - func_volatile: Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - DefineType: When creating a new data type
  - AlterType: When modifying an existing data type
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Receive functions can have either 1 argument (internal) or 3 arguments (internal, oid, int4)
- The function reports an error if both signature forms exist simultaneously to avoid ambiguity
- The receive function must return exactly the target type being defined
- A warning is issued if the function is marked as volatile, as receive functions should typically be stable or immutable
- This complements the type's send function to provide binary I/O capabilities for network transmission and storage