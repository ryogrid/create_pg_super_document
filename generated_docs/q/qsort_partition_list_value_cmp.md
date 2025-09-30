# qsort_partition_list_value_cmp

## Location
[src/backend/partitioning/partbounds.c:3793-3809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3793-L3809)

## Overview
A comparison function used by qsort to compare two list partition bound datums using the partition key's comparison function.

## Definition
```c
static int32 qsort_partition_list_value_cmp(const void *a, const void *b, void *arg)
```

## Detailed Description
This function serves as a qsort comparison callback for sorting PartitionListValue structures. It extracts the Datum values from two PartitionListValue structures and compares them using the appropriate comparison function stored in the PartitionKey. The comparison is performed by calling the partition support function (partsupfunc) with the appropriate collation, ensuring that list partition values are sorted according to the correct data type semantics.

## Parameters / Member Variables
- `a`: Pointer to the first PartitionListValue structure to compare
- `b`: Pointer to the second PartitionListValue structure to compare  
- `arg`: Pointer to PartitionKey structure containing comparison function and collation information

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionListValue](../P/PartitionListValue.md)
  - [PartitionKey](../P/PartitionKey.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
- Called from (representative examples):
  - compare_range_bounds
  - [create_list_bounds](../c/create_list_bounds.md)

## Notes and Other Information
- This is a static function internal to partbounds.c
- Used specifically for sorting list partition values during partition boundary processing
- Relies on the partition key's support function (partsupfunc[0]) to perform type-appropriate comparisons
- Uses the partition key's collation (partcollation[0]) for collation-sensitive data types
- Returns negative, zero, or positive value following standard qsort comparison conventions
- The function signature includes a void *arg parameter to accommodate qsort_r style sorting with context

## Simplified Source

```c
static int32 qsort_partition_list_value_cmp(const void *a, const void *b, void *arg) {
    // Extract Datum values from list value structures
    Datum val1 = ((const PartitionListValue *) a)->value;
    Datum val2 = ((const PartitionListValue *) b)->value;
    PartitionKey key = (PartitionKey) arg;

    // Compare using partition key's comparison function and collation
    return DatumGetInt32(FunctionCall2Coll(&key->partsupfunc[0],
                                          key->partcollation[0],
                                          val1, val2));
}
```