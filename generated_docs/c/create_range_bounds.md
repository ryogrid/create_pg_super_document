# create_range_bounds

## Location
[src/backend/partitioning/partbounds.c:677-895](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L677-L895)

## Overview
Creates a PartitionBoundInfo structure for a range partitioned table by processing boundary specifications and organizing them into a unified, sorted structure.

## Definition

```c
static PartitionBoundInfo
create_range_bounds(PartitionBoundSpec **boundspecs, int nparts,
					PartitionKey key, int **mapping)
```
## Detailed Description
This function takes an array of partition boundary specifications for range partitions and creates a unified PartitionBoundInfo structure. It processes both lower and upper bounds from all partitions, sorts them, removes duplicates, and creates the final boundary structure with proper indexing. The function handles default partitions specially and assigns canonical indexes to each partition.

The function creates a comprehensive boundary structure by:
1. Extracting both lower and upper bounds from each partition specification
2. Creating a unified list of all bounds across partitions
3. Sorting bounds in ascending order using partition-specific comparison
4. Removing duplicate bounds to create a distinct set
5. Building the final PartitionBoundInfo with proper indexing

## Parameters / Member Variables
- : Array of PartitionBoundSpec pointers containing the boundary specifications for each partition
- : Number of partitions being processed
- : PartitionKey containing partitioning metadata (comparison functions, data types, etc.)
- : Pointer to mapping array that will be updated to map original partition indexes to canonical indexes

## Dependencies
- Functions called/Symbols referenced:
  - [make_one_partition_rbound](../m/make_one_partition_rbound.md)
  - [qsort_partition_rbound_cmp](../q/qsort_partition_rbound_cmp.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [datumCopy](../d/datumCopy.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
- Data types used:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md)
  - [PartitionBoundSpec](../P/PartitionBoundSpec.md)
  - [PartitionRangeBound](../P/PartitionRangeBound.md)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md)
- Called from:
  - [partition_bounds_create](../p/partition_bounds_create.md)
  - compare_range_bounds

## Notes and Other Information
- The function handles default partitions by noting their index but not adding bounds to the processing array
- Bounds are stored with -1 indexes for lower bounds since they represent gaps between partitions
- The final indexes array includes an extra -1 element at the end
- Memory allocation is optimized by allocating single large arrays for datums and kinds rather than many small arrays
- The function ensures all partitions receive canonical indexes from 0 to nparts-1

## Simplified Source

```c
static PartitionBoundInfo
create_range_bounds(PartitionBoundSpec **boundspecs, int nparts,
                    PartitionKey key, int **mapping)
{
    PartitionBoundInfo boundinfo;
    PartitionRangeBound **rbounds = NULL;
    PartitionRangeBound **all_bounds, *prev;
    int i, k, partnatts;
    int ndatums = 0;
    int default_index = -1;
    int next_index = 0;
    Datum *boundDatums;
    PartitionRangeDatumKind *boundKinds;

    // Initialize bound info structure
    boundinfo = (PartitionBoundInfoData *) palloc0(sizeof(PartitionBoundInfoData));
    boundinfo->strategy = key->strategy;
    boundinfo->null_index = -1;  // No special null-accepting range partition
    boundinfo->default_index = -1;

    // Create unified list of range bounds from all partitions
    all_bounds = (PartitionRangeBound **) palloc0(2 * nparts * sizeof(PartitionRangeBound *));

    for (i = 0; i < nparts; i++)
    {
        PartitionBoundSpec *spec = boundspecs[i];
        PartitionRangeBound *lower, *upper;

        if (spec->strategy != PARTITION_STRATEGY_RANGE)
            elog(ERROR, "invalid strategy in partition bound spec");

        // Handle default partition
        if (spec->is_default)
        {
            default_index = i;
            continue;
        }

        // Create lower and upper bounds for this partition
        lower = make_one_partition_rbound(key, i, spec->lowerdatums, true);
        upper = make_one_partition_rbound(key, i, spec->upperdatums, false);
        all_bounds[ndatums++] = lower;
        all_bounds[ndatums++] = upper;
    }

    // Sort all bounds in ascending order
    qsort_arg(all_bounds, ndatums, sizeof(PartitionRangeBound *),
              qsort_partition_rbound_cmp, key);

    // Remove duplicate bounds to create distinct set
    rbounds = (PartitionRangeBound **) palloc(ndatums * sizeof(PartitionRangeBound *));
    k = 0;
    prev = NULL;

    for (i = 0; i < ndatums; i++)
    {
        PartitionRangeBound *cur = all_bounds[i];
        bool is_distinct = false;
        int j;

        // Check if current bound is distinct from previous
        for (j = 0; j < key->partnatts; j++)
        {
            if (prev == NULL || cur->kind[j] != prev->kind[j])
            {
                is_distinct = true;
                break;
            }

            // Compare values if both are actual values
            if (cur->kind[j] == PARTITION_RANGE_DATUM_VALUE)
            {
                Datum cmpval = FunctionCall2Coll(&key->partsupfunc[j],
                                                key->partcollation[j],
                                                cur->datums[j],
                                                prev->datums[j]);
                if (DatumGetInt32(cmpval) != 0)
                {
                    is_distinct = true;
                    break;
                }
            }
            else
                break;  // MINVALUE or MAXVALUE, treat as equal
        }

        if (is_distinct)
            rbounds[k++] = all_bounds[i];

        prev = cur;
    }

    pfree(all_bounds);
    ndatums = k;  // Update to count of distinct datums

    // Set up bound info arrays
    boundinfo->ndatums = ndatums;
    boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));
    boundinfo->kind = (PartitionRangeDatumKind **) palloc(ndatums * sizeof(PartitionRangeDatumKind *));
    boundinfo->nindexes = ndatums + 1;  // Extra -1 element
    boundinfo->indexes = (int *) palloc((ndatums + 1) * sizeof(int));

    // Allocate single arrays for efficiency
    partnatts = key->partnatts;
    boundDatums = (Datum *) palloc(ndatums * partnatts * sizeof(Datum));
    boundKinds = (PartitionRangeDatumKind *) palloc(ndatums * partnatts * sizeof(PartitionRangeDatumKind));

    // Copy bounds and assign indexes
    for (i = 0; i < ndatums; i++)
    {
        int j;

        boundinfo->datums[i] = &boundDatums[i * partnatts];
        boundinfo->kind[i] = &boundKinds[i * partnatts];

        for (j = 0; j < partnatts; j++)
        {
            if (rbounds[i]->kind[j] == PARTITION_RANGE_DATUM_VALUE)
                boundinfo->datums[i][j] = datumCopy(rbounds[i]->datums[j],
                                                   key->parttypbyval[j],
                                                   key->parttyplen[j]);
            boundinfo->kind[i][j] = rbounds[i]->kind[j];
        }

        // Lower bounds get -1 (gaps between partitions)
        if (rbounds[i]->lower)
            boundinfo->indexes[i] = -1;
        else
        {
            // Upper bounds get canonical partition index
            int orig_index = rbounds[i]->index;
            if ((*mapping)[orig_index] == -1)
                (*mapping)[orig_index] = next_index++;
            boundinfo->indexes[i] = (*mapping)[orig_index];
        }
    }

    pfree(rbounds);

    // Handle default partition
    if (default_index != -1)
    {
        Assert(default_index >= 0 && (*mapping)[default_index] == -1);
        (*mapping)[default_index] = next_index++;
        boundinfo->default_index = (*mapping)[default_index];
    }

    // Add final -1 element
    boundinfo->indexes[i] = -1;

    Assert(next_index == nparts);
    return boundinfo;
}
```