# equal

## Location
[src/backend/nodes/equalfuncs.c:223-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/equalfuncs.c#L223-L264)

## Overview
The  function is PostgreSQL's primary node comparison function that determines whether two parse tree nodes are structurally equivalent in all their significant attributes.

## Definition


## Detailed Description
The  function provides deep structural comparison of PostgreSQL parse tree nodes, implementing the core equality semantics used throughout the query processing system. It performs a comprehensive comparison that checks not only the node types but also recursively compares all significant fields within the nodes.

The function employs a multi-stage comparison strategy:
1. **Identity check**: Returns true immediately if both pointers reference the same object
2. **NULL handling**: Returns false if exactly one parameter is NULL
3. **Type validation**: Compares node tags to ensure both nodes are of the same type
4. **Stack overflow protection**: Guards against deeply nested expressions that could cause stack overflow
5. **Dispatch mechanism**: Uses a switch statement with generated comparison functions for each node type
6. **List handling**: Special case handling for various list types (List, IntList, OidList, XidList)

The actual comparison logic for specific node types is generated and included via "equalfuncs.switch.c", which contains type-specific comparison functions that handle the detailed field-by-field comparison for each node type.

## Parameters / Member Variables
- : First node to compare (const void pointer, typically cast from a Node*)
- : Second node to compare (const void pointer, typically cast from a Node*)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts the node type tag for type checking
  - : Guards against stack overflow in deeply nested comparisons  
  - : Handles comparison of list-type nodes
  - Generated comparison functions (via equalfuncs.switch.c)

- Called from (representative examples):
  - : List membership testing
  - : List element removal
  - : Equivalence class processing in query optimization
  - : Expression matching in equivalence classes
  - : Expression equality testing in optimizer
  - : Target list membership testing
  - : Window function processing
  - Various optimizer, parser, and executor components

## Notes and Other Information
- **Performance**: The identity check (a == b) provides fast-path optimization for identical node references
- **Recursion safety**: Uses  to prevent stack overflow on deeply nested expressions
- **Generated code**: Most type-specific comparison logic is auto-generated in equalfuncs.switch.c
- **List types**: Special handling for PostgreSQL's various list node types (List, IntList, OidList, XidList)
- **Error handling**: Throws ERROR for unrecognized node types, indicating a programming error
- **Usage context**: Fundamental to query plan caching, optimization transformations, and semantic analysis
- **Thread safety**: Function is read-only and thread-safe when used with immutable node structures