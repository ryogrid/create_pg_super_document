# llvm_split_symbol_name

## Location
[src/backend/jit/llvm/llvmjit.c:1145-1178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/jit/llvm/llvmjit.c#L1145-L1178)

## Overview
Parses PostgreSQL external symbol names and splits them into separate module and function name components for dynamic symbol resolution.

## Definition

```c
void
llvm_split_symbol_name(const char *name, char **modname, char **funcname)
```
## Detailed Description
This function analyzes symbol names to determine whether they represent functions from external modules or functions from the main binary/external libraries. It follows PostgreSQL's naming convention for external module functions, which use the format .

The function operates by:
1. Checking if the symbol name starts with the "pgextern." prefix
2. If it does, extracting the module name and function name by finding the last dot separator
3. If it doesn't, treating the entire name as a function name with no associated module

This parsing is essential for the JIT compilation system to correctly resolve symbols from PostgreSQL extensions and external libraries.

## Parameters / Member Variables
- : Input symbol name to be parsed
- : Output parameter that receives the module name (NULL if not an external module function)  
- : Output parameter that receives the function name

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library)
  -  (standard C library)
  -  (standard C library - finds last occurrence)
  -  (PostgreSQL string duplication with length limit)
  -  (PostgreSQL string duplication)
  -  (PostgreSQL assertion macro)
- Called from:
  -  at src/backend/jit/llvm/llvmjit.c:1195

## Notes and Other Information
- The function handles PostgreSQL's external module symbol naming convention where module symbols are prefixed with "pgextern."
- For external module functions, the format is: 
- Symbol names cannot contain periods, which allows the function to reliably split on the first and last period occurrences
- Memory for both  and  is allocated using PostgreSQL's memory management functions
- If the symbol is not from an external module,  is set to NULL and the entire name becomes the function name
- This function is a key component in PostgreSQL's dynamic symbol resolution for JIT-compiled code