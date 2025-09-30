# create_list_bounds

## Location
[src/backend/partitioning/partbounds.c:462-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L462-L676)

## Overview
Creates a PartitionBoundInfo structure specifically for list partitioned tables, converting list partition bound specifications into the internal representation with support for null, default, and interleaved partitions.

## Definition
```c
static PartitionBoundInfo create_list_bounds(PartitionBoundSpec **boundspecs, int nparts, PartitionKey key, int **mapping)
```

## Detailed Description
This function implements list partition bounds creation by processing an array of list partition specifications and building the comprehensive internal PartitionBoundInfo structure. The function handles the complexities of list partitioning including:

1. Processing non-null values from all partitions into a unified sorted array
2. Handling special partitions: NULL-accepting and DEFAULT partitions
3. Creating canonical index mappings for efficient partition lookup
4. Detecting and marking interleaved partitions for optimization purposes
5. Building the datums array with properly copied partition values

The function first counts non-null datums, creates a unified PartitionListValue array, sorts it using the partition key's comparison function, and then builds the final PartitionBoundInfo structure with proper index mappings. It also performs sophisticated analysis to detect interleaved partitions where multiple partitions may contain overlapping or out-of-order values.

## Parameters / Member Variables
- `boundspecs`: Array of PartitionBoundSpec pointers containing list partition specifications with their listdatums
- `nparts`: Number of list partitions to process
- `key`: PartitionKey containing the partitioning strategy, type information, and comparison functions
- `mapping`: Output parameter - array mapping original partition indexes to canonical sorted indexes

## Dependencies
- Functions called/Symbols referenced:
  - [get_non_null_list_datum_count](../g/get_non_null_list_datum_count.md)
  - [palloc0](../p/palloc0.md)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - qsort_arg
  - [qsort_partition_list_value_cmp](../q/qsort_partition_list_value_cmp.md)
  - [datumCopy](../d/datumCopy.md)
  - partition_bound_accepts_nulls
  - partition_bound_has_default
  - [bms_add_member](../b/bms_add_member.md)
  - lfirst_node
  - foreach
  - PARTITION_STRATEGY_LIST
  - [PartitionListValue](../P/PartitionListValue.md)
  - [PartitionBoundInfoData](../P/PartitionBoundInfoData.md)
- Called from (representative examples):
  - [partition_bounds_create](../p/partition_bounds_create.md) (src/backend/partitioning/partbounds.c:332)

## Notes and Other Information
- Static function, only accessible within partbounds.c
- Supports NULL partitions (partitions that accept NULL values) with null_index tracking
- Supports DEFAULT partitions (catch-all partitions) with default_index tracking
- Implements sophisticated interleaved partition detection for query optimization
- Uses single large memory allocation for boundDatums array for efficiency
- Validates that only one partition can accept NULL values
- Performs deep copying of partition values using datumCopy to ensure memory safety
- The interleaved_parts bitmap identifies partitions that may contain overlapping value ranges
- Canonical indexing enables efficient binary search and partition elimination during query planning
- Essential component for list partition constraint checking and partition pruning

## Simplified Source

```c
static PartitionBoundInfo
create_list_bounds(PartitionBoundSpec **boundspecs, int nparts,
                   PartitionKey key, int **mapping)
{
    PartitionBoundInfo boundinfo;
    PartitionListValue *all_values;
    int i, j;
    int ndatums;
    int next_index = 0;
    int default_index = -1;
    int null_index = -1;
    Datum *boundDatums;

    // Initialize bound info structure
    boundinfo = (PartitionBoundInfoData *) palloc0(sizeof(PartitionBoundInfoData));
    boundinfo->strategy = key->strategy;
    boundinfo->null_index = -1;
    boundinfo->default_index = -1;

    // Count and collect non-null values from all partitions
    ndatums = get_non_null_list_datum_count(boundspecs, nparts);
    all_values = (PartitionListValue *) palloc(ndatums * sizeof(PartitionListValue));

    for (j = 0, i = 0; i < nparts; i++)
    {
        PartitionBoundSpec *spec = boundspecs[i];
        ListCell *c;

        if (spec->strategy != PARTITION_STRATEGY_LIST)
            elog(ERROR, "invalid strategy in partition bound spec");

        // Handle default partition
        if (spec->is_default)
        {
            default_index = i;
            continue;
        }

        // Process list values
        foreach(c, spec->listdatums)
        {
            Const *val = lfirst_node(Const, c);

            if (!val->constisnull)
            {
                all_values[j].index = i;
                all_values[j].value = val->constvalue;
                j++;
            }
            else
            {
                // Track null-accepting partition
                if (null_index != -1)
                    elog(ERROR, "found null more than once");
                null_index = i;
            }
        }
    }

    Assert(j == ndatums);

    // Sort values using partition key comparison
    qsort_arg(all_values, ndatums, sizeof(PartitionListValue),
              qsort_partition_list_value_cmp, key);

    // Set up bound info arrays
    boundinfo->ndatums = ndatums;
    boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));
    boundinfo->nindexes = ndatums;
    boundinfo->indexes = (int *) palloc(ndatums * sizeof(int));
    boundDatums = (Datum *) palloc(ndatums * sizeof(Datum));

    // Copy values and assign canonical indexes
    for (i = 0; i < ndatums; i++)
    {
        int orig_index = all_values[i].index;

        boundinfo->datums[i] = &boundDatums[i];
        boundinfo->datums[i][0] = datumCopy(all_values[i].value,
                                           key->parttypbyval[0],
                                           key->parttyplen[0]);

        // Assign canonical index mapping
        if ((*mapping)[orig_index] == -1)
            (*mapping)[orig_index] = next_index++;

        boundinfo->indexes[i] = (*mapping)[orig_index];
    }

    pfree(all_values);

    // Handle special partitions
    if (null_index != -1)
    {
        if ((*mapping)[null_index] == -1)
            (*mapping)[null_index] = next_index++;
        boundinfo->null_index = (*mapping)[null_index];
    }

    if (default_index != -1)
    {
        Assert((*mapping)[default_index] == -1);
        (*mapping)[default_index] = next_index++;
        boundinfo->default_index = (*mapping)[default_index];
    }

    // Detect interleaved partitions for optimization
    if (nparts > 1)
    {
        // Check if partitions have overlapping or out-of-order values
        if (boundinfo->ndatums +
            partition_bound_accepts_nulls(boundinfo) +
            partition_bound_has_default(boundinfo) != nparts)
        {
            int last_index = -1;
            for (i = 0; i < boundinfo->nindexes; i++)
            {
                int index = boundinfo->indexes[i];
                if (index < last_index ||
                    (partition_bound_accepts_nulls(boundinfo) &&
                     index == boundinfo->null_index))
                {
                    boundinfo->interleaved_parts =
                        bms_add_member(boundinfo->interleaved_parts, index);
                }
                last_index = index;
            }
        }

        // Default partition is always considered interleaved
        if (partition_bound_has_default(boundinfo))
            boundinfo->interleaved_parts =
                bms_add_member(boundinfo->interleaved_parts,
                              boundinfo->default_index);
    }

    Assert(next_index == nparts);
    return boundinfo;
}
```