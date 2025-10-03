# BuildSpeculativeIndexInfo

## Location
[src/backend/catalog/index.c:2642-2701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2642-L2701)

## Overview
BuildSpeculativeIndexInfo augments an IndexInfo structure with additional metadata required for speculative insertion operations on unique indexes.

## Definition

```c
void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii)
```
## Detailed Description
BuildSpeculativeIndexInfo extends an existing IndexInfo structure with specialized information needed to support speculative insertion in unique B-tree indexes. This function is specifically designed for PostgreSQL's speculative insertion mechanism, which allows for optimistic insertion followed by uniqueness checking. The function allocates and populates arrays for unique operators, procedure OIDs, and strategy numbers that are used during the speculative insertion process. This processing is done separately from BuildIndexInfo() to avoid overhead in common non-speculative cases, ensuring optimal performance for regular index operations.

## Parameters / Member Variables
- `index`: Relation structure representing the index being prepared for speculative insertion
- `*ii`: IndexInfo structure to be augmented with speculative insertion metadata
## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfo](../I/IndexInfo.md) (structure type)
  - IndexRelationGetNumberOfKeyAttributes (function)
  - [get_opfamily_member](../g/get_opfamily_member.md) (function) 
  - [get_opcode](../g/get_opcode.md) (function)
- Called from (representative examples):
  - [ExecOpenIndices](../E/ExecOpenIndices.md)

## Notes and Other Information
- Only supports B-tree indexes (BTREE_AM_OID) and will error for other access methods
- Requires the index to be unique (asserted with ii->ii_Unique)
- Allocates memory for three arrays: ii_UniqueOps, ii_UniqueProcs, and ii_UniqueStrats
- Uses BTEqualStrategyNumber strategy for all key attributes
- Performs validation to ensure required operators exist in the opfamily
- This function is part of PostgreSQL's speculative insertion optimization that reduces lock contention during concurrent unique constraint checking

## Simplified Source

```c
void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii)
{
    int indnkeyatts;
    int i;

    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);

    // Verify this is a unique B-tree index
    Assert(ii->ii_Unique);

    if (index->rd_rel->relam != BTREE_AM_OID)
        elog(ERROR, "unexpected non-btree speculative unique index");

    // Allocate arrays for uniqueness checking metadata
    ii->ii_UniqueOps = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
    ii->ii_UniqueProcs = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
    ii->ii_UniqueStrats = (uint16 *) palloc(sizeof(uint16) * indnkeyatts);

    // Populate arrays with equality operators and procedures
    for (i = 0; i < indnkeyatts; i++)
    {
        ii->ii_UniqueStrats[i] = BTEqualStrategyNumber;

        // Get equality operator from opfamily
        ii->ii_UniqueOps[i] = get_opfamily_member(index->rd_opfamily[i],
                                                  index->rd_opcintype[i],
                                                  index->rd_opcintype[i],
                                                  ii->ii_UniqueStrats[i]);

        if (!OidIsValid(ii->ii_UniqueOps[i]))
            elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
                 ii->ii_UniqueStrats[i], index->rd_opcintype[i],
                 index->rd_opcintype[i], index->rd_opfamily[i]);

        // Get procedure OID for the operator
        ii->ii_UniqueProcs[i] = get_opcode(ii->ii_UniqueOps[i]);
    }
}
```