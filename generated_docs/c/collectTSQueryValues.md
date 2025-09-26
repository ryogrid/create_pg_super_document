# collectTSQueryValues

## Location
[src/backend/utils/adt/tsquery_op.c:267-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L267-L297)

## Overview
Extracts all textual values from a TSQuery structure, creating an array of null-terminated strings representing the query operands.

## Definition
```c
static char **collectTSQueryValues(TSQuery a, int *nvalues_p)
```

## Detailed Description
This function iterates through all items in a TSQuery structure and extracts the string values (operands) from query items of type QI_VAL. It allocates memory for each extracted value, copies the string data, and returns an array of pointers to these null-terminated strings. The function is used internally for text search query processing operations, particularly for containment checks between queries.

## Parameters / Member Variables
- `a`: The TSQuery structure to process
- `nvalues_p`: Pointer to an integer that will receive the count of extracted values

## Dependencies
- Functions called/Symbols referenced:
  - GETQUERY (macro to access query items array)
  - GETOPERAND (macro to access operand data)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)
- Types referenced:
  - TSQuery (text search query structure)
  - QueryItem (individual query element structure)  
  - QI_VAL (query item type constant for values)
- Called from (representative examples):
  - [tsq_mcontains](../t/tsq_mcontains.md) (at src/backend/utils/adt/tsquery_op.c:318-319)

## Notes and Other Information
- This is a static function, only accessible within the tsquery_op.c module
- Memory is allocated using PostgreSQL's palloc system, which is automatically freed at transaction end
- The function only processes QI_VAL type query items, skipping operators and other item types
- [String](../S/String.md) lengths are determined from the qoperand.length field and copied from the operand data at the specified distance offset
- The returned array contains exactly `*nvalues_p` elements, all properly null-terminated strings