# makeJsonTablePathSpec

## Location
[src/backend/nodes/makefuncs.c:977-997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L977-L997)

## Overview
Creates a JsonTablePathSpec node for specifying path expressions used in JSON_TABLE operations, which define how to navigate and extract data from JSON documents.

## Definition
```c
JsonTablePathSpec *makeJsonTablePathSpec(char *string, char *name, int string_location, int name_location)
```

## Detailed Description
The `makeJsonTablePathSpec` function is a constructor that creates and initializes a `JsonTablePathSpec` node. This node type is used in PostgreSQL's SQL/JSON implementation specifically for the JSON_TABLE function, which allows querying JSON data as if it were a relational table. The function takes a path string (which defines the JSON path expression), an optional name for the path, and location information for both components. The path string is converted to a string constant node, and the name is duplicated using PostgreSQL's memory management functions.

## Parameters / Member Variables
- `string`: A character pointer to the JSON path expression string (required, cannot be NULL)
- `name`: A character pointer to an optional name for the path specification (can be NULL)
- `string_location`: An integer representing the source location of the path string in the original query
- `name_location`: An integer representing the source location of the name in the original query

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (PostgreSQL node allocation macro)
  - [makeStringConst](makeStringConst.md) (function to create string constant nodes)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - Assert (PostgreSQL assertion macro)
  - [JsonTablePathSpec](../J/JsonTablePathSpec.md) (node type structure)
- Called from (representative examples):
  - Referenced in makefuncs.h header file

## Notes and Other Information
This function is specifically designed for PostgreSQL's JSON_TABLE functionality, which implements the SQL/JSON standard's JSON_TABLE expression. The path specification created by this function defines how to navigate through JSON documents to extract tabular data. The function includes an assertion to ensure the path string is not NULL, as a valid path is essential for JSON_TABLE operations. The use of makeStringConst for the path string ensures proper integration with PostgreSQL's expression evaluation system, while pstrdup for the name ensures proper memory management in PostgreSQL's memory contexts.