# makeBitString

## Location
[src/backend/nodes/value.c:77-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/value.c#L77-L83)

## Overview
The makeBitString function creates a new BitString node containing a specified bit string value, used for representing bit string literals in PostgreSQL's parse tree structure.

## Definition
BitString *makeBitString(char *str)

## Detailed Description
makeBitString is a factory function that allocates and initializes a new BitString node in PostgreSQL's node system. It uses the makeNode macro to create a properly initialized node with the correct NodeTag, then assigns the provided string representation of the bit string to the node. This function is part of PostgreSQL's value node system, designed specifically to handle bit string literals (e.g., B'101010' or X'FF') that appear in SQL statements.

BitString nodes represent bit string constants as they are parsed and processed through PostgreSQL's parser and planner. The bit string is stored as a string representation rather than binary data, allowing for precise preservation of the original literal format and enabling the value to participate in PostgreSQL's node-based architecture.

The caller is responsible for ensuring that the str parameter is a palloc'd (PostgreSQL-allocated) string, as the BitString node will take ownership of this memory without making a copy.

## Parameters / Member Variables
- `str`: A palloc'd string containing the bit string representation (e.g., "101010" for binary or "FF" for hexadecimal). The caller must ensure this memory is allocated with palloc().

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation and initialization)
  - [BitString](../B/BitString.md) (struct type definition)
- Called from (representative examples):
  - [nodeRead](../n/nodeRead.md) (in read.c for deserialization)
  - Referenced in strVal macro (in value.h header)

## Notes and Other Information
- Part of PostgreSQL's value node system alongside makeInteger, makeFloat, makeBoolean, and makeString
- Specifically designed for handling SQL bit string literals (B'..' and X'..' syntax)
- Stores bit strings as string representation rather than binary data for parsing flexibility
- Less commonly used compared to other value node types, primarily for bit string literal processing
- The caller must provide a palloc'd string - the function does not copy the input string
- Enables bit string values to be stored in Lists and participate in standard node operations
- Located in src/backend/nodes/value.c as part of the core value node creation infrastructure
- Used primarily during lexical analysis and parsing of bit string constants