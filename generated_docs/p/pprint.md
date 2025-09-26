# pprint

## Location
src/backend/nodes/print.c: 54 - 71

## Overview
A utility function that pretty-prints the contents of any PostgreSQL Node to stdout with enhanced formatting for better readability.

## Definition
```c
void pprint(const void *obj)
```

## Detailed Description
The `pprint` function is a debugging utility similar to `print` but with enhanced formatting for improved readability. It takes any PostgreSQL Node object and outputs its complete structure to stdout using pretty-printing formatting. The function converts the node to a string representation with location information, applies pretty formatting to make the output more readable, and prints it with a newline. Like `print`, it properly manages memory by freeing intermediate string representations.

## Parameters / Member Variables
- `obj`: A pointer to the Node object to be pretty-printed (can be any PostgreSQL node type)

## Dependencies
- Functions called/Symbols referenced:
  - nodeToStringWithLocations: Converts the node to a string representation with location information
  - pretty_format_node_dump: Applies pretty formatting to the node string for enhanced readability
  - pfree: Frees allocated memory
  - printf: Standard C library function for output
  - fflush: Ensures output is immediately displayed

- Called from (representative examples):
  - set_rel_pathlist: Used in optimizer path selection debugging
  - standard_join_search: Used in join planning debugging
  - generate_partitionwise_join_paths: Used in partitioned table join debugging
  - preprocess_expression: Used in expression preprocessing debugging
  - nodeDisplay: Header declaration and macro usage

## Notes and Other Information
- This function is primarily used for debugging complex optimizer operations
- The pretty formatting makes complex node structures more readable than regular `print`
- Output includes location information when available
- Memory is properly managed with pfree calls to prevent leaks
- Output is flushed immediately to ensure visibility during debugging
- Commonly used in optimizer debugging contexts where readability is crucial
- Located in src/backend/nodes/print.c:54-71