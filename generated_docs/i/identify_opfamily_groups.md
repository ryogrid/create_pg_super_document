# identify_opfamily_groups

## Location
src/backend/access/index/amvalidate.c: 43 - 151

## Overview
Groups operators and support functions by datatype combinations within an operator family, creating a structured representation for validation purposes.

## Definition


## Detailed Description
This function analyzes an operator family's operators and support functions to create OpFamilyOpFuncGroup structures. Each group represents a unique lefttype/righttype datatype combination and tracks which operator strategies and support function numbers are present using bitmasks. The function processes ordered catalog lists concurrently, ensuring all operators and functions for each datatype pair are grouped together. Strategy numbers and function numbers are stored as bits in uint64 fields, supporting up to 63 different strategies/functions per group.

## Parameters / Member Variables
- `oprlist`: CatCList of operators (pg_amop entries) for the operator family, must be ordered
- `proclist`: CatCList of support functions (pg_amproc entries) for the operator family, must be ordered

## Dependencies
- Functions called/Symbols referenced:
  - CatCList (catalog cache list structure)
  - [OpFamilyOpFuncGroup](../O/OpFamilyOpFuncGroup.md) (result structure type)
  - Form_pg_amop (operator tuple form)
  - Form_pg_amproc (support function tuple form)
  - GETSTRUCT (macro to extract tuple structure)
  - [palloc](../p/palloc.md) (memory allocation)
  - lappend (list append function)
- Called from (representative examples):
  - [brinvalidate](../b/brinvalidate.md)
  - [ginvalidate](../g/ginvalidate.md)
  - [gistvalidate](../g/gistvalidate.md)
  - [hashvalidate](../h/hashvalidate.md)
  - [btvalidate](../b/btvalidate.md)
  - [spgvalidate](../s/spgvalidate.md)

## Notes and Other Information
- Requires ordered catalog lists to function correctly; will error if lists are unordered
- Uses concurrent advancement through both lists to maintain efficiency
- Supports operator strategies and function numbers 1-63 (bit positions in uint64)
- Critical component of access method validation infrastructure
- Located in src/backend/access/index/amvalidate.c:43-151