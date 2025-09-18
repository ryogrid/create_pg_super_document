# makeJsonTablePath

## Location
src/backend/nodes/makefuncs.c: 998 - 1007

## Overview
Creates and initializes a JsonTablePath node that represents a JSON path expression used in evaluating JSON_TABLE plan nodes.

## Definition


## Detailed Description
The  function is a constructor function that creates a new JsonTablePath node structure. This function is part of PostgreSQL's node creation utilities and is specifically designed to support the JSON_TABLE functionality. It allocates memory for a new JsonTablePath structure using the  macro and initializes its fields with the provided path value and optional path name.

The function includes an assertion to verify that the pathvalue parameter is indeed a Const node, ensuring type safety during construction. This is consistent with PostgreSQL's practice of runtime type checking for node operations.

## Parameters / Member Variables
- : A Const node containing the JSON path expression string that will be evaluated
- : An optional character string representing the name associated with this path (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation)
  - IsA (macro for type checking)
  - Assert (assertion macro)
  - JsonTablePath (the struct type being created)
  - Const (input parameter type)

- Called from (representative examples):
  - [makeJsonTablePathScan](makeJsonTablePathScan.md) (in src/backend/parser/parse_jsontable.c:515)

## Notes and Other Information
- This function is defined in src/backend/nodes/makefuncs.c:998-1007
- The function is declared in src/include/nodes/makefuncs.h:122
- JsonTablePath is a simple structure with just two fields: the path value (Const) and an optional name (char *)
- The function follows PostgreSQL's standard pattern for node constructor functions using the makeNode infrastructure
- This is part of the JSON_TABLE feature implementation which allows SQL queries to extract data from JSON documents in a tabular format
- The created JsonTablePath nodes are used during query planning and execution to represent path expressions that need to be evaluated against JSON data