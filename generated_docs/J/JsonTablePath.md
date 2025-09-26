# JsonTablePath

## Location
[src/include/nodes/primnodes.h:1867-1873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1867-L1873)

## Overview
JsonTablePath represents a JSON path expression that is computed as part of evaluating a JSON_TABLE plan node.

## Definition
```c
typedef struct JsonTablePath
{
    NodeTag     type;
    Const      *value;
    char       *name;
} JsonTablePath;
```

## Detailed Description
JsonTablePath is a simple node structure that encapsulates a named JSON path expression used within JSON_TABLE operations. It stores both the path value as a constant and an associated name for referencing the path within the JSON_TABLE execution context.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JsonTablePath node
- `value`: Const node containing the JSON path expression value
- `name`: String name for referencing this path within the JSON_TABLE context

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - Const (no direct references from this symbol)
- Called from (representative examples):
  - makeJsonTablePathSpec
  - makeJsonTablePath
  - JsonTablePathScan

## Notes and Other Information
- Simple container structure for JSON path expressions within JSON_TABLE operations
- Part of the JSON_TABLE implementation infrastructure
- Used to organize and reference multiple path expressions within a single JSON_TABLE query
- The name field allows for path reuse and reference within complex JSON_TABLE structures