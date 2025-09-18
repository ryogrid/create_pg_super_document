# ObjectAddressStack

## Location
src/backend/catalog/dependency.c: 124 - 129

## Overview
A threaded list structure used for recursion detection during dependency traversal in PostgreSQL's dependency system.

## Definition


## Detailed Description
ObjectAddressStack is a linked list structure that maintains a stack of ObjectAddress objects currently being processed during dependency analysis. This structure is crucial for preventing infinite recursion when traversing object dependencies, as it tracks which objects are already being visited in the current call chain. Each stack entry contains a pointer to an ObjectAddress being processed, associated flags indicating the processing state, and a pointer to the next level in the stack hierarchy.

## Parameters / Member Variables
- `object`: Pointer to the ObjectAddress currently being visited in the dependency traversal
- `flags`: Integer containing flag bits that track the current processing state and properties of the object
- `next`: Pointer to the next outer level in the stack, forming a linked list of nested dependency contexts

## Dependencies
- Functions called/Symbols referenced:
  - ObjectAddress (referenced through the object pointer)
- Called from (representative examples):
  - stack_address_present_add_flags
  - findDependentObjects
  - find_expr_references_context

## Notes and Other Information
- Used specifically in src/backend/catalog/dependency.c for dependency analysis
- The stack grows as dependency traversal goes deeper into nested object relationships
- Essential for cycle detection in PostgreSQL's object dependency graph
- Works in conjunction with ObjectAddress structures to provide complete context during dependency resolution
- The flags member uses dependency flag bits (like DEPFLAG_SUBOBJECT) to track processing state