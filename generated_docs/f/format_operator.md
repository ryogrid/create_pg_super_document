# format_operator

## Location
src/backend/utils/adt/regproc.c: 793 - 798

## Overview
A simple wrapper function that converts an operator OID to its standard textual representation using default formatting options.

## Definition


## Detailed Description
The  function provides a simplified interface to operator formatting by calling  with default flags (0). This function is the most commonly used operator formatting function in PostgreSQL, providing standard behavior without special formatting requirements.

The function generates output in the format "opr_name(lefttype,righttype)" with schema qualification applied only when necessary for disambiguation based on the current search_path visibility.

## Parameters / Member Variables
- : The OID of the operator to format

## Dependencies
- Functions called/Symbols referenced:
  - format_operator_extended
- Called from (representative examples):
  - brinvalidate (src/backend/access/brin/brin_validate.c)
  - ginvalidate (src/backend/access/gin/ginvalidate.c)
  - gistvalidate (src/backend/access/gist/gistvalidate.c)
  - hashvalidate (src/backend/access/hash/hashvalidate.c)
  - btvalidate (src/backend/access/nbtree/nbtvalidate.c)
  - spgvalidate (src/backend/access/spgist/spgvalidate.c)
  - getObjectDescription (src/backend/catalog/objectaddress.c)
  - ComputeIndexAttrs (src/backend/commands/indexcmds.c)
  - regoperatorout (src/backend/utils/adt/regproc.c)

## Notes and Other Information
- This is a convenience wrapper around format_operator_extended with no special flags
- Widely used throughout PostgreSQL for operator validation and error reporting
- Returns a palloc'd string that must be freed by the caller
- Uses default behavior: schema qualification only when necessary, numeric OID returned for invalid operators
- Located in src/backend/utils/adt/regproc.c:793-798