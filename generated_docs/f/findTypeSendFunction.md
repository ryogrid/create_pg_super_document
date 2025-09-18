# findTypeSendFunction

## Location
src/backend/commands/typecmds.c: 2105 - 2139

## Overview
Validates and retrieves the OID of a type's send function, which converts internal binary representation to external binary format for network transmission and storage.

## Definition
```c
static Oid findTypeSendFunction(List *procname, Oid typeOid)
```

## Detailed Description
This function locates and validates a type send function during type definition or modification. Type send functions are crucial components that convert PostgreSQL's internal binary representation of a data type to its external binary format (bytea). This external binary format is used for network transmission between PostgreSQL instances and for certain storage operations. The function ensures the specified function meets PostgreSQL's requirements for send functions, including proper signature and return type validation.

## Parameters / Member Variables
- `procname`: A list representing the qualified name of the send function to look up
- `typeOid`: The OID of the data type for which this will serve as the send function

## Dependencies
- Functions called/Symbols referenced:
  - LookupFuncName: Looks up function by name with specified argument types
  - func_signature_string: Creates a string representation of function signature for error messages
  - get_func_rettype: Retrieves the return type OID of a function
  - NameListToString: Converts a name list to string format for display
  - func_volatile: Checks the volatility category of a function
  - PROVOLATILE_VOLATILE: Constant representing volatile function category
- Called from (representative examples):
  - DefineType: When creating a new data type
  - AlterType: When modifying an existing data type
  - AlterTypeRecurseParams: When modifying type parameters

## Notes and Other Information
- Send functions must take exactly one argument of the target type and return bytea
- The function issues an error if the specified function doesn't exist or has wrong return type
- A warning is issued (not an error) if the function is marked as volatile, as send functions should typically be stable or immutable
- This complements the type's receive function to provide complete binary I/O capabilities
- The binary format produced by send functions must be compatible with the corresponding receive function