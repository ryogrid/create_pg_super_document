# sequence_options

## Location
src/backend/commands/sequence.c: 1707 - 1740

## Overview
Retrieves sequence parameters from the system catalog and formats them as a list of DefElem nodes in the same format used by the SQL parser.

## Definition


## Detailed Description
The  function queries the pg_sequence system catalog to retrieve all parameters for a given sequence and converts them into a standardized list format. This function is essential for operations that need to reconstruct or display sequence options, such as table inheritance operations where sequence properties need to be copied.

The function retrieves the following sequence parameters from pg_sequence:
- **cache**: Number of sequence values to cache in memory
- **cycle**: Whether the sequence should cycle when reaching min/max values
- **increment**: Step size for sequence value generation
- **maxvalue**: Maximum value the sequence can reach
- **minvalue**: Minimum value the sequence can reach  
- **start**: Starting value for the sequence

All numeric values are converted to string format using INT64_FORMAT and wrapped in Float nodes, matching the parser's behavior for handling large integers. The boolean cycle parameter is wrapped in a Boolean node.

## Parameters / Member Variables
- : Object identifier of the sequence relation whose options are to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - lappend
  - makeDefElem
  - [makeFloat](../m/makeFloat.md)
  - [psprintf](../p/psprintf.md)
  - [makeBoolean](../m/makeBoolean.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from:
  - [transformTableLikeClause](../t/transformTableLikeClause.md)

## Notes and Other Information
- The function uses the system cache (SEQRELID) for efficient lookup of sequence metadata
- All integer values are converted to Float nodes using psprintf with INT64_FORMAT to handle 64-bit sequence values properly
- The returned list format matches exactly what the SQL parser creates, enabling seamless integration with parsing and DDL operations
- Memory management follows PostgreSQL conventions with proper cache tuple release
- Error handling includes cache lookup failure detection with appropriate error logging
- Located in src/backend/commands/sequence.c:1707-1740