# pushValue

## Location
[src/backend/utils/adt/tsquery.c:580-615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L580-L615)

## Overview
Public function that processes and pushes a string operand onto the parser state's polish notation stack, handling CRC calculation and buffer management for the operand storage.

## Definition
```c
void
pushValue(TSQueryParserState state, char *strval, int lenval, int16 weight, bool prefix)
```

## Detailed Description
This function serves as the primary interface for adding string operands to tsquery parsing operations. It performs length validation, calculates a CRC32 checksum of the operand string, and delegates to pushValue_internal for actual stack manipulation. Additionally, it manages the internal operand buffer (state->op) by copying the string value and automatically expanding the buffer when necessary.

The function implements a comprehensive operand storage system where strings are copied into a continuously growing buffer with null termination. This approach ensures that all operand strings remain accessible throughout the parsing process while maintaining memory efficiency through buffer reallocation as needed.

## Parameters / Member Variables
- `state`: TSQueryParserState containing parsing context, operand buffer, and stack state
- `strval`: Pointer to the string value to be pushed (must equal state->curop according to contract)
- `lenval`: Length of the string value
- `weight`: Weight flags for the operand (bit mask for text search weight classes)
- `prefix`: Boolean flag indicating whether this operand supports prefix matching

## Dependencies
- Functions called/Symbols referenced:
  - [pushValue_internal](pushValue_internal.md) (internal implementation for stack operations)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation function)
  - memcpy (standard C memory copy function)
  - ereturn (PostgreSQL error return macro)
  - INIT_LEGACY_CRC32, COMP_LEGACY_CRC32, FIN_LEGACY_CRC32 (CRC calculation macros)
  - MAXSTRLEN (PostgreSQL string length limit constant)
  - pg_crc32 (PostgreSQL CRC32 type)
- Called from (representative examples):
  - [pushval_morph](pushval_morph.md)
  - [pushval_asis](pushval_asis.md)
  - P_TSQ_WEB

## Notes and Other Information
- This is the public interface for pushing operands, while pushValue_internal handles the actual stack manipulation
- The function includes buffer management that automatically doubles the operand buffer size when space is needed
- CRC calculation uses PostgreSQL's legacy CRC32 implementation for consistency with existing query structures
- The operand buffer (state->op) stores all operand strings contiguously with null terminators
- Error handling validates string length against MAXSTRLEN before processing
- The distance calculation (state->curop - [state](../s/state.md)->op) represents the offset of the current operand in the buffer
- Buffer management includes proper pointer arithmetic to maintain the curop position after reallocation
- The sumlen field tracks the total length of all stored operands plus their null terminators