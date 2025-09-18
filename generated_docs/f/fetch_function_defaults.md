# fetch_function_defaults

## Location
src/backend/optimizer/util/clauses.c: 4350 - 4379

## Overview
Retrieves and parses a function's default argument expressions from its pg_proc tuple.

## Definition


## Detailed Description
This function extracts default argument expressions from a function's pg_proc system catalog entry. The default arguments are stored in the proargdefaults field as a serialized string representation of an expression tree.

The function performs the following operations:
1. **Attribute retrieval**: Gets the proargdefaults attribute from the pg_proc tuple using SysCacheGetAttrNotNull
2. **String conversion**: Converts the stored Datum to a C string representation
3. **Deserialization**: Parses the string back into a List of expression nodes using stringToNode
4. **Memory cleanup**: Frees the temporary string to prevent memory leaks

The returned list contains expression nodes that represent the default values for the function's parameters that have defaults defined.

## Parameters / Member Variables
- : The function's pg_proc tuple containing the default argument definitions

## Dependencies
- Functions called/Symbols referenced:
  - SysCacheGetAttrNotNull
  - TextDatumGetCString
  - stringToNode
- Called from (representative examples):
  - reorder_function_arguments
  - add_function_defaults

## Notes and Other Information
- The function assumes that proargdefaults is not NULL (uses SysCacheGetAttrNotNull)
- Default expressions are stored as serialized node trees in the system catalog
- The returned expressions may need further processing (like constant evaluation) before use
- Memory for the temporary string is properly cleaned up after deserialization
- The function returns a List where each element corresponds to a default expression for parameters that have defaults
- This is a low-level utility function used by higher-level argument processing functions