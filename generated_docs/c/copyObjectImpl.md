# copyObjectImpl

## Location
[src/backend/nodes/copyfuncs.c:177-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/copyfuncs.c#L177-L212)

## Overview
The core implementation of PostgreSQL's generic node copying system that creates deep copies of Node trees or lists by dispatching to appropriate type-specific copy functions.

## Definition

```c
void *
copyObjectImpl(const void *from)
```
## Detailed Description
The  function serves as the central dispatcher for PostgreSQL's node copying infrastructure. It creates deep copies of arbitrary Node trees by examining the node type tag and delegating to the appropriate type-specific copy function. The function handles three main categories of objects: regular nodes (dispatched via an auto-generated switch statement), lists (with different strategies for deep vs shallow copying), and provides error handling for unrecognized types.

The function includes stack depth checking to prevent stack overflow from overly complex expression trees, which is crucial for handling deeply nested parse trees that could potentially exhaust stack space during recursive copying operations.

## Parameters / Member Variables
- : Pointer to the source object to be copied (can be any Node type or list)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](check_stack_depth.md) (prevents stack overflow during deep recursion)
  - nodeTag (determines the type of the node being copied)
  - copyfuncs.switch.c (auto-generated switch cases for all node types)
  - [list_copy_deep](../l/list_copy_deep.md) (for deep copying of generic lists)
  - [list_copy](../l/list_copy.md) (for shallow copying of integer/OID/XID lists)
  - elog (for error reporting on unrecognized node types)
- Called from (representative examples):
  - copyObject (public interface macro/inline function)
  - COPY_NODE_FIELD (macro for copying node fields)
  - [list_copy_deep](../l/list_copy_deep.md) (for recursive copying within lists)

## Notes and Other Information
- This function is the implementation behind the public  interface
- Uses an auto-generated switch statement (copyfuncs.switch.c) that includes cases for all known node types
- Differentiates between deep copying (T_List) and shallow copying (T_IntList, T_OidList, T_XidList) based on content type
- Stack overflow protection is essential due to the recursive nature of deep copying complex expression trees  
- The function returns void* to maintain genericity while working with PostgreSQL's type-tagged node system
- Error handling ensures that unrecognized node types are caught and reported rather than causing silent corruption