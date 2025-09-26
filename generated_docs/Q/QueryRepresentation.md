# QueryRepresentation

## Location
[src/backend/utils/adt/tsrank.c:554-555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L554-L555)

## Overview
A container structure that combines a text search query with corresponding operand data, used for efficient text search ranking calculations in PostgreSQL.

## Definition
```c
typedef struct
{
    TSQuery         query;
    QueryRepresentationOperand *operandData;
} QueryRepresentation;
```

## Detailed Description
QueryRepresentation serves as a unified data structure that pairs a TSQuery (parsed text search query) with an array of QueryRepresentationOperand structures containing positional and existence information for each operand in the query. This design enables efficient ranking calculations by providing direct access to both the query structure and the corresponding document-specific operand data.

The structure is fundamental to PostgreSQL text search ranking algorithms, particularly those that consider word positions and frequencies when calculating relevance scores. It acts as an intermediate representation that bridges the gap between the abstract query structure and concrete document analysis results.

## Parameters / Member Variables
- `query`: The TSQuery structure containing the parsed text search query with its logical structure and operands
- `operandData`: Pointer to an array of QueryRepresentationOperand structures, one for each operand in the query, containing position and existence data for the current document being ranked

## Dependencies
- Functions called/Symbols referenced:
  - TSQuery (core text search query type)
  - [QueryRepresentationOperand](QueryRepresentationOperand.md) (operand-specific data structure)
- Called from (representative examples):
  - [checkcondition_QueryOperand](../c/checkcondition_QueryOperand.md)
  - [resetQueryRepresentation](../r/resetQueryRepresentation.md)
  - [fillQueryRepresentationData](../f/fillQueryRepresentationData.md)
  - [Cover](../C/Cover.md)
  - [get_docrep](../g/get_docrep.md)
  - [calc_rank_cd](../c/calc_rank_cd.md)

## Notes and Other Information
- The operandData array size corresponds to query->size, providing one-to-one mapping between query operands and their document-specific data
- Used with the QR_GET_OPERAND_DATA macro for efficient operand data access during ranking
- Central to the text search ranking infrastructure, enabling position-based relevance scoring
- Memory management for operandData array is handled by calling functions, typically allocated during ranking initialization