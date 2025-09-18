# find_variable

## Location
src/interfaces/ecpg/preproc/variable.c: 193 - 259

## Overview
Locates and returns a variable structure from the global variable list, handling complex variable references including arrays and struct members with proper error reporting for undeclared variables.

## Definition


## Detailed Description
The `find_variable` function is a comprehensive variable lookup mechanism in the ECPG preprocessor that handles various forms of variable references. It can process simple variable names, array accesses with brackets, and struct/union member access using dot notation. The function intelligently parses the variable name to determine the appropriate lookup strategy and creates new variable structures for complex expressions when needed.

The function will terminate the program with a fatal error if the requested variable is not found, ensuring that all variable references are properly validated during preprocessing. It supports nested array access and struct member access, creating appropriate type information for the resulting variable references.

## Parameters / Member Variables
- `name`: The variable name to search for, which may include array indices ([]) and struct member access (.) operators

## Dependencies
- Functions called/Symbols referenced:
  - find_struct: Handles struct/union member lookup
  - find_simple: Performs simple variable name lookup
  - new_variable: Creates new variable structures for complex expressions
  - ECPGmake_array_type: Creates array type structures
  - ECPGmake_struct_type: Creates struct/union type structures
  - ECPGmake_simple_type: Creates simple type structures
  - mmfatal: Reports fatal errors and terminates execution
- Called from (representative examples):
  - ECPGnumeric_lvalue: When processing numeric lvalue expressions
  - output_get_descr: During descriptor output generation
  - output_set_descr: During descriptor setting operations
  - ECPGdump_a_type: When dumping type information
  - find_struct: For recursive struct member lookup

## Notes and Other Information
- The function modifies the input string temporarily during parsing but restores it
- Supports nested array access by counting bracket pairs to find the end of array expressions
- Automatically creates new variable structures for array elements and struct members with appropriate type information
- Uses fatal error reporting to ensure all variable references are valid, preventing runtime errors
- Handles complex type hierarchies including arrays of structs and nested structures
- The parsing logic distinguishes between array access (`[`) and struct member access (`.`) operators