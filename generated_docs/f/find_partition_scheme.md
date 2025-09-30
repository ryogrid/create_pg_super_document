# find_partition_scheme

## Location
[src/backend/optimizer/util/plancat.c:2449-2555](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2449-L2555)

## Overview
Finds an existing PartitionScheme that matches a relation's partitioning characteristics, or creates a new one if no match is found, for use in partition-aware query planning.

## Definition

```c
structs themselves for they might
		 * be different across PartitionKey's, so just Assert for the function
		 * OIDs.
		 */
#ifdef USE_ASSERT_CHECKING
		for (i = 0;
```
## Detailed Description
This static function implements a caching mechanism for partition schemes by searching through existing schemes in the PlannerInfo's part_schemes list and returning a match if found, or creating and caching a new scheme if no match exists.

The matching process compares multiple partitioning characteristics:
- Partitioning strategy (e.g., RANGE, HASH, LIST)
- Number of partition key attributes
- Operator family OIDs for each partition key
- Input type OIDs for each partition key  
- Collation OIDs for each partition key
- Type length and byval properties (verified by assertion when other properties match)
- Partition support function OIDs (verified by assertion)

When creating a new scheme, the function allocates memory and copies all relevant partitioning metadata from the relation's PartitionKey to ensure the scheme persists beyond the relation's lifecycle. The new scheme is added to the planner's part_schemes list for future reuse.

## Parameters / Member Variables
- : PlannerInfo structure containing the list of existing partition schemes
- : The partitioned relation whose partition scheme is needed

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - memcpy
  - [fmgr_info_copy](fmgr_info_copy.md)
  - [lappend](../l/lappend.md)
  - [PartitionKey](../P/PartitionKey.md)
  - [PartitionScheme](../P/PartitionScheme.md)
  - [PartitionSchemeData](../P/PartitionSchemeData.md)
- Called from (representative examples):
  - [set_relation_partition_info](../s/set_relation_partition_info.md)

## Notes and Other Information
- This is a static function only used within plancat.c
- The function assumes the relation is partitioned and has a valid partition key
- Caching partition schemes improves performance when multiple relations share the same partitioning characteristics
- All partition scheme data is copied to ensure it survives beyond the relation cache entry's lifetime
- The scheme comparison is comprehensive, ensuring that only truly equivalent partitioning configurations are matched
- Memory for the partition scheme is allocated in the current memory context and persists for the duration of planning

## Simplified Source

```c
static PartitionScheme
find_partition_scheme(PlannerInfo *root, Relation relation)
{
    PartitionKey partkey = RelationGetPartitionKey(relation);
    int partnatts = partkey->partnatts;
    PartitionScheme part_scheme;
    ListCell *lc;

    // Search for existing matching partition scheme
    foreach(lc, root->part_schemes)
    {
        part_scheme = lfirst(lc);

        // Check if partition characteristics match
        if (partkey->strategy == part_scheme->strategy &&
            partnatts == part_scheme->partnatts &&
            partition_properties_match(partkey, part_scheme, partnatts))
        {
            return part_scheme;  // Found existing scheme
        }
    }

    // Create new partition scheme
    part_scheme = create_new_partition_scheme(partkey, partnatts);
    root->part_schemes = lappend(root->part_schemes, part_scheme);
    return part_scheme;

    // Helper: Check if partition properties match
    static bool
    partition_properties_match(PartitionKey partkey, PartitionScheme scheme, int natts)
    {
        return (memcmp(partkey->partopfamily, scheme->partopfamily,
                      sizeof(Oid) * natts) == 0 &&
                memcmp(partkey->partopcintype, scheme->partopcintype,
                      sizeof(Oid) * natts) == 0 &&
                memcmp(partkey->partcollation, scheme->partcollation,
                      sizeof(Oid) * natts) == 0);
    }

    // Helper: Create new partition scheme with copied metadata
    static PartitionScheme
    create_new_partition_scheme(PartitionKey partkey, int natts)
    {
        PartitionScheme scheme = palloc0(sizeof(PartitionSchemeData));
        scheme->strategy = partkey->strategy;
        scheme->partnatts = natts;

        // Copy partition metadata arrays
        scheme->partopfamily = copy_oid_array(partkey->partopfamily, natts);
        scheme->partopcintype = copy_oid_array(partkey->partopcintype, natts);
        scheme->partcollation = copy_oid_array(partkey->partcollation, natts);
        scheme->parttyplen = copy_int16_array(partkey->parttyplen, natts);
        scheme->parttypbyval = copy_bool_array(partkey->parttypbyval, natts);

        // Copy support functions
        scheme->partsupfunc = palloc(sizeof(FmgrInfo) * natts);
        for (int i = 0; i < natts; i++)
            fmgr_info_copy(&scheme->partsupfunc[i], &partkey->partsupfunc[i],
                          CurrentMemoryContext);

        return scheme;
    }
}
```