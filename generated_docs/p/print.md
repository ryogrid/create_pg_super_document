# print

## Location
[src/interfaces/ecpg/test/expected/preproc-whenever.c:27-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/test/expected/preproc-whenever.c#L27-L32)

## Overview
The print function outputs the contents of a PostgreSQL Node structure to stdout in a human-readable format.

## Definition


## Detailed Description
The print function is a debugging utility that converts a PostgreSQL Node structure to its string representation and prints it to stdout. It uses the nodeToStringWithLocations function to serialize the node into a string format, then applies formatting through format_node_dump to make the output more readable. The function automatically adds a newline and flushes stdout to ensure immediate output visibility. Memory allocated during the conversion process is properly freed to prevent leaks.

## Parameters / Member Variables
- : A pointer to the Node structure to be printed. Can be any PostgreSQL node type cast to void pointer.

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md) (converts node to string with location info)
  - format_node_dump (formats the string representation)
  - [pfree](pfree.md) (memory deallocation)
  - printf (standard output)
  - fflush (output buffer flushing)
- Called from (representative examples):
  - nodeDisplay (from header file)
  - Various test and debugging contexts in ECPG

## Notes and Other Information
- This function is primarily used for debugging and development purposes
- Located in src/backend/nodes/print.c:36-53
- Part of PostgreSQL's node system infrastructure
- Automatically handles memory management for temporary strings
- Ensures output is immediately visible by flushing stdout