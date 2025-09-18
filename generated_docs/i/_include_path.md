# _include_path

## Location
src/interfaces/ecpg/preproc/type.h: 130 - 135

## Overview
The  structure implements a linked list data structure used in the ECPG (Embedded SQL in C) preprocessor to manage and store directory paths for header file inclusion during preprocessing.

## Definition


## Detailed Description
 is a self-referential structure that forms the foundation of a singly-linked list used by the ECPG preprocessor to maintain a collection of directory paths where header files should be searched during the preprocessing phase. This structure enables the preprocessor to traverse through multiple include directories in a specific order when resolving #include directives, similar to how compilers handle include path resolution.

## Parameters / Member Variables
- : A character pointer containing the directory path string to be included in the search path
- : A pointer to the next  structure in the linked list, forming the chain of include directories

## Dependencies
- Functions called/Symbols referenced:
  - [_include_path](_include_path.md) (self-reference for the linked list structure)
- Called from (representative examples):
  - [add_include_path](../a/add_include_path.md) (in src/interfaces/ecpg/preproc/ecpg.c)
  - [main](../m/main.md) (in src/interfaces/ecpg/preproc/ecpg.c)

## Notes and Other Information
- This structure is part of the ECPG preprocessor's include management system located in 
- Implements a classic singly-linked list pattern for dynamic path collection
- Used actively in the main ECPG preprocessor functions for path management
- The linked list design allows for flexible addition and traversal of include directories
- Memory management is required for both the path strings and the list nodes themselves
- The structure supports the standard compiler behavior of searching include directories in a specified order