# fillQueryRepresentationData

## Location
[src/backend/utils/adt/tsrank.c:606-645](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L606-L645)

## Overview
Populates a QueryRepresentation structure with positional data from a DocRepresentation entry, tracking operand existence and positions for text search ranking calculations.

## Definition
static void fillQueryRepresentationData(QueryRepresentation *qr, DocRepresentation *entry)

## Detailed Description
This function processes a DocRepresentation entry and updates the corresponding QueryRepresentation structure with operand position information. It iterates through all query items in the entry, identifies valid operands (QI_VAL type), and records their positions in the operand data array. The function handles both forward and reverse insertion modes and avoids duplicate position entries for the same word position.

## Parameters / Member Variables
- `qr`: Pointer to the QueryRepresentation structure to be filled with data
- `entry`: Pointer to the DocRepresentation entry containing query items and position information

## Dependencies
- Functions called/Symbols referenced:
  - DocRepresentation (struct type)
  - QueryRepresentation (struct type)
  - QueryRepresentationOperand (struct type)
  - QI_VAL (constant for query item type)
  - QR_GET_OPERAND_DATA (macro for accessing operand data)
  - MAXQROPOS (maximum query representation positions)
  - WEP_GETPOS (macro for extracting position from word entry position)
- Called from (representative examples):
  - [Cover](../C/Cover.md) (called at lines 667 and 697)

## Notes and Other Information
This is a static function within the tsrank.c module that plays a crucial role in building the data structures needed for text search ranking. The function carefully manages position arrays considering insertion direction and prevents duplicate entries for the same word position. The MAXQROPOS limit controls the maximum number of positions that can be stored per operand.