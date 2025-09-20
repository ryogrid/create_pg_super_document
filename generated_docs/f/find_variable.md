# find_variable

## Location
[src/interfaces/ecpg/preproc/variable.c:193-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L193-L259)

## Overview
Locates and returns a variable structure from the global variable list, handling complex variable references including arrays and struct members with proper error reporting for undeclared variables.

## Definition

```c
struct variable *p;
```
## Detailed Description
The `find_variable` function is a comprehensive variable lookup mechanism in the ECPG preprocessor that handles various forms of variable references. It can process simple variable names, array accesses with brackets, and struct/union member access using dot notation. The function intelligently parses the variable name to determine the appropriate lookup strategy and creates new variable structures for complex expressions when needed.

The function will terminate the program with a fatal error if the requested variable is not found, ensuring that all variable references are properly validated during preprocessing. It supports nested array access and struct member access, creating appropriate type information for the resulting variable references.

## Parameters / Member Variables
- `name`: The variable name to search for, which may include array indices ([]) and struct member access (.) operators

## Dependencies
- Functions called/Symbols referenced:
  - find_struct: Handles struct/union member lookup
  - find_simple: Performs simple variable name lookup
  - [new_variable](../n/new_variable.md): Creates new variable structures for complex expressions
  - [ECPGmake_array_type](../E/ECPGmake_array_type.md): Creates array type structures
  - [ECPGmake_struct_type](../E/ECPGmake_struct_type.md): Creates struct/union type structures
  - [ECPGmake_simple_type](../E/ECPGmake_simple_type.md): Creates simple type structures
  - mmfatal: Reports fatal errors and terminates execution
- Called from (representative examples):
  - [ECPGnumeric_lvalue](../E/ECPGnumeric_lvalue.md): When processing numeric lvalue expressions
  - [output_get_descr](../o/output_get_descr.md): During descriptor output generation
  - [output_set_descr](../o/output_set_descr.md): During descriptor setting operations
  - [ECPGdump_a_type](../E/ECPGdump_a_type.md): When dumping type information
  - find_struct: For recursive struct member lookup

## Notes and Other Information
- The function modifies the input string temporarily during parsing but restores it
- Supports nested array access by counting bracket pairs to find the end of array expressions
- Automatically creates new variable structures for array elements and struct members with appropriate type information
- Uses fatal error reporting to ensure all variable references are valid, preventing runtime errors
- Handles complex type hierarchies including arrays of structs and nested structures
- The parsing logic distinguishes between array access (`[`) and struct member access (`.`) operators