# check_new_partition_bound

## Location
[src/backend/partitioning/partbounds.c:2896-3250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2896-L3250)

## Overview
Validates that a new partition's bounds do not overlap with any existing partitions and enforces partition strategy-specific constraints.

## Definition
```c
void check_new_partition_bound(char *relname, Relation parent,
                             PartitionBoundSpec *spec, ParseState *pstate)
```

## Detailed Description
This comprehensive validation function ensures partition bound integrity when adding new partitions. For DEFAULT partitions, it checks for existing defaults. For HASH partitions, it enforces the modulus factor rule (each modulus must be a factor of larger moduli) and detects remainder conflicts. For LIST partitions, it searches for duplicate values including NULL handling. For RANGE partitions, it validates bound ordering, checks for empty ranges, and uses binary search to detect overlaps with existing partitions. The function provides detailed error messages with source location information for debugging.

## Parameters / Member Variables
- `relname`: Name of the new partition being created
- `parent`: Parent partitioned table relation
- `spec`: Partition bound specification containing bounds and strategy
- `pstate`: Parse state for error reporting with location information

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetPartitionKey](../R/RelationGetPartitionKey.md)
  - [RelationGetPartitionDesc](../R/RelationGetPartitionDesc.md)
  - partition_bound_has_default
  - [partition_hash_bsearch](../p/partition_hash_bsearch.md)
  - [partition_list_bsearch](../p/partition_list_bsearch.md)
  - [partition_range_bsearch](../p/partition_range_bsearch.md)
  - [make_one_partition_rbound](../m/make_one_partition_rbound.md)
  - [partition_rbound_cmp](../p/partition_rbound_cmp.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [PartitionBoundSpec](../P/PartitionBoundSpec.md)
  - [PartitionKey](../P/PartitionKey.md)
  - [PartitionDesc](../P/PartitionDesc.md)
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
  - [PartitionRangeDatum](../P/PartitionRangeDatum.md)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
- Called from (representative examples):
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md)

## Notes and Other Information
- This is a public function (non-static) used by table creation and partition attachment commands
- Implements comprehensive validation for all PostgreSQL partition strategies (HASH, LIST, RANGE)
- For HASH partitions, enforces the mathematical constraint that moduli form a factorization chain
- Provides precise error location reporting for syntax and semantic errors
- Critical for maintaining partition constraint integrity and preventing data inconsistencies
- Located in src/backend/partitioning/partbounds.c:2896-3250

## Simplified Source

```c
void check_new_partition_bound(char *relname, Relation parent,
                              PartitionBoundSpec *spec, ParseState *pstate) {
    PartitionKey key = RelationGetPartitionKey(parent);
    PartitionDesc partdesc = RelationGetPartitionDesc(parent, false);
    PartitionBoundInfo boundinfo = partdesc->boundinfo;
    bool overlap = false;
    int overlap_location = -1;
    int with = -1;

    // Handle DEFAULT partition
    if (spec->is_default) {
        if (boundinfo && partition_bound_has_default(boundinfo)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("partition \"%s\" conflicts with existing default partition", relname)));
        }
        return;
    }

    switch (key->strategy) {
        case PARTITION_STRATEGY_HASH:
            // Check hash modulus factor rules and remainder conflicts
            if (partdesc->nparts > 0) {
                int offset = partition_hash_bsearch(boundinfo, spec->modulus, spec->remainder);

                // Validate modulus factor constraint
                if (offset >= 0) {
                    int prev_modulus = DatumGetInt32(boundinfo->datums[offset][0]);
                    if (spec->modulus % prev_modulus != 0) {
                        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                errmsg("every hash partition modulus must be a factor of the next larger modulus")));
                    }
                }

                // Check for remainder conflicts
                int remainder = spec->remainder;
                int greatest_modulus = boundinfo->nindexes;
                if (remainder >= greatest_modulus)
                    remainder = remainder % greatest_modulus;

                do {
                    if (boundinfo->indexes[remainder] != -1) {
                        overlap = true;
                        with = boundinfo->indexes[remainder];
                        break;
                    }
                    remainder += spec->modulus;
                } while (remainder < greatest_modulus);
            }
            break;

        case PARTITION_STRATEGY_LIST:
            // Check for duplicate list values
            if (partdesc->nparts > 0) {
                ListCell *cell;
                foreach(cell, spec->listdatums) {
                    Const *val = lfirst_node(Const, cell);

                    if (!val->constisnull) {
                        int offset;
                        bool equal;
                        offset = partition_list_bsearch(&key->partsupfunc[0],
                                                       key->partcollation, boundinfo,
                                                       val->constvalue, &equal);
                        if (offset >= 0 && equal) {
                            overlap = true;
                            with = boundinfo->indexes[offset];
                            break;
                        }
                    } else if (partition_bound_accepts_nulls(boundinfo)) {
                        overlap = true;
                        with = boundinfo->null_index;
                        break;
                    }
                }
            }
            break;

        case PARTITION_STRATEGY_RANGE:
            // Check for range overlaps
            if (partdesc->nparts > 0) {
                PartitionRangeBound *lower = make_one_partition_rbound(key, -1,
                                                                      spec->lowerdatums, true);
                PartitionRangeBound *upper = make_one_partition_rbound(key, -1,
                                                                      spec->upperdatums, false);

                // Validate bounds create non-empty range
                int cmpval = partition_rbound_cmp(key->partnatts, key->partsupfunc,
                                                 key->partcollation, lower->datums,
                                                 lower->kind, true, upper);
                if (cmpval > 0) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("empty range bound specified for partition \"%s\"", relname)));
                }

                // Check for overlaps with existing partitions
                int offset = partition_range_bsearch(key->partnatts, key->partsupfunc,
                                                    key->partcollation, boundinfo,
                                                    lower, &cmpval);

                if (boundinfo->indexes[offset + 1] >= 0) {
                    overlap = true;
                    with = boundinfo->indexes[offset + 1];
                }
            }
            break;
    }

    if (overlap) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("partition \"%s\" would overlap partition \"%s\"",
                       relname, get_rel_name(partdesc->oids[with]))));
    }
}
```