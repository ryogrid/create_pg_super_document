# fmNodePtr

## Location
[src/include/fmgr.h:22-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fmgr.h#L22-L22)

## Overview
fmNodePtr is a typedef that represents a pointer to a Node structure, used as a stub reference in the function manager system to avoid including primnodes.h.

## Definition


## Detailed Description
fmNodePtr is a forward declaration typedef defined in fmgr.h that creates a pointer type to the Node structure without requiring the full definition of Node. This design pattern is used to avoid circular dependencies and reduce compilation overhead by not including primnodes.h in the function manager header. The Node structure is a fundamental part of PostgreSQL's parse tree and expression evaluation system, and fmNodePtr allows the function manager to reference nodes without exposing the full node implementation details.

## Parameters / Member Variables
- This is a simple typedef with no parameters or member variables

## Dependencies
- Functions called/Symbols referenced:
  - [Node](../N/Node.md) (struct - forward declaration only)
- Called from (representative examples):
  - [FmgrInfo](../F/FmgrInfo.md) (uses as fn_expr field type)
  - [FunctionCallInfoBaseData](../F/FunctionCallInfoBaseData.md) (uses as context and resultinfo field types)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - [DirectInputFunctionCallSafe](../D/DirectInputFunctionCallSafe.md)
  - OidFunctionCall9

## Notes and Other Information
- This typedef serves as an abstraction layer to avoid including primnodes.h in fmgr.h
- The actual Node structure definition is found in primnodes.h
- Used primarily in function call contexts where expression nodes need to be passed
- Part of PostgreSQL's modular header design to minimize compilation dependencies