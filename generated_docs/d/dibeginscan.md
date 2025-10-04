# dibeginscan

## Location
[src/test/modules/dummy_index_am/dummy_index_am.c:246-258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/dummy_index_am/dummy_index_am.c#L246-L258)

## Overview
Begins an index scan operation for the dummy index access method, a test module that provides a minimal implementation of PostgreSQL's index access method interface.

## Definition
static IndexScanDesc dibeginscan(Relation r, int nkeys, int norderbys)

## Detailed Description
This function is part of the dummy index access method implementation used for testing purposes. It initializes an index scan by calling the standard PostgreSQL function RelationGetIndexScan() to create and return an IndexScanDesc structure. The function serves as a placeholder implementation that demonstrates the minimal requirements for an index AM's scan initialization routine.

As part of a test module, this function doesn't perform any actual indexing work but provides the necessary interface compliance for PostgreSQL's index access method framework.

## Parameters / Member Variables
- r: The relation (index) being scanned
- nkeys: Number of scan keys (search conditions) 
- norderbys: Number of order-by expressions for the scan

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexScan](../R/RelationGetIndexScan.md)
- Data types used:
  - [IndexScanDesc](../I/IndexScanDesc.md)
- Called from (representative examples):
  - [dihandler](dihandler.md)

## Notes and Other Information
- This is a static function within the dummy_index_am test module
- Located in src/test/modules/dummy_index_am/dummy_index_am.c:246-258
- The function includes a comment indicating it's "pretending" to do work, emphasizing its test/placeholder nature
- Part of PostgreSQL's extensible index access method framework testing infrastructure

## Simplified Source

```c
static IndexScanDesc
dibeginscan(Relation r, int nkeys, int norderbys)
{
    // Initialize index scan using standard PostgreSQL function
    IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);
    return scan;
}
```