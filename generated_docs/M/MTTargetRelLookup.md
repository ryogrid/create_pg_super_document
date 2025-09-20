# MTTargetRelLookup

## Location
[src/backend/executor/nodeModifyTable.c:75-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L75-L79)

## Overview
MTTargetRelLookup is a hash table entry structure used by ModifyTable execution nodes to efficiently map relation OIDs to their corresponding indexes in the resultRelInfo array during UPDATE and DELETE operations on inherited tables.

## Definition

```c
typedef struct MTTargetRelLookup
{
	Oid			relationOid;	/* hash key, must be first */
	int			relationIndex;	/* rel's index in resultRelInfo[] array */
} MTTargetRelLookup;
```
## Detailed Description
This structure serves as a hash table entry for optimizing target relation lookups in ModifyTable operations when dealing with inherited UPDATE and DELETE queries. When a ModifyTable node needs to process many target relations (typically in inheritance hierarchies or partitioned tables), PostgreSQL creates a hash table using this structure to avoid linear searches through the resultRelInfo array.

The structure is designed specifically for hash table usage - the relationOid field must be first as it serves as the hash key. The hash table is populated during ModifyTable initialization and used during execution to quickly locate the appropriate ResultRelInfo for a given table OID.

## Parameters / Member Variables
- : The OID of the target relation, serving as the hash key for fast lookups
- : The corresponding index in the ModifyTableState's resultRelInfo[] array where the relation's ResultRelInfo is stored

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [ExecLookupResultRelByOid](../E/ExecLookupResultRelByOid.md) (uses this structure in hash table lookups)

## Notes and Other Information
- This optimization is only used when there are many target relations; for few relations, a simple linear search through resultRelInfo[] is performed instead
- The hash table using this structure is stored in ModifyTableState.mt_resultOidHash
- The structure layout is critical - relationOid must be the first field to serve as the hash key
- This is part of PostgreSQL's optimization for handling inheritance hierarchies and partitioned tables efficiently during modification operations