# push_null_elements

## Location
[src/backend/utils/adt/jsonfuncs.c:1700-1718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1700-L1718)

## Overview
A utility function that pushes a specified number of null JSON elements into a JSONB parse state structure.

## Definition


## Detailed Description
This function creates and pushes null JSON values as array elements into a JSONB parse state. It's used internally to fill array positions with null values, typically when constructing or modifying JSONB arrays where certain positions need to be explicitly set to null. The function operates by creating a JsonbValue of type jbvNull and repeatedly pushing it as an array element using the WJB_ELEM flag.

## Parameters / Member Variables
-   PID TTY          TIME CMD
 1177 ?        00:00:00 bash
 1203 ?        00:00:00 ps: Pointer to a JsonbParseState pointer that tracks the current parsing/building state
- : The number of null elements to push into the parse state

## Dependencies
- Functions called/Symbols referenced:
  - [pushJsonbValue](pushJsonbValue.md)
  - [JsonbParseState](../J/JsonbParseState.md) (type)
  - jbvNull (enum value)
  - WJB_ELEM (enum value)
- Called from (representative examples):
  - [push_path](push_path.md)
  - [setPathArray](../s/setPathArray.md)

## Notes and Other Information
This is a static helper function used internally within jsonfuncs.c for JSONB manipulation operations. It's particularly useful when building arrays where gaps need to be filled with null values or when extending arrays to a specific length.