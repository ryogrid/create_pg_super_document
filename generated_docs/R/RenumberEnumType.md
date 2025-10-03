# RenumberEnumType

## Location
[src/backend/catalog/pg_enum.c:761-796](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L761-L796)

## Overview
Renumbers existing enum elements to have consecutive sort positions (1..n), typically done only when necessary to resolve sort order conflicts when adding new enum labels.

## Definition

```c
static void
RenumberEnumType(Relation pg_enum, HeapTuple *existing, int nelems)
```
## Detailed Description
This function performs a critical but rarely-used operation of renumbering all enum values in an enum type to have consecutive sort positions starting from 1. The renumbering is done reluctantly because:

1. **Concurrency concerns**: Updating existing pg_enum entries creates potential hazards for other backends reading the catalog concurrently
2. **MVCC complexity**: While catalog scans use MVCC semantics, syscache machinery might read different entries under different snapshots
3. **Performance impact**: The operation requires updating multiple catalog tuples and incrementing the command counter

The function works backwards (from highest index to lowest) to avoid uniqueness constraint violations during the renumbering process. Each enum value gets a new sort order of (index + 1), ensuring proper sequential ordering.

The renumbering is triggered only when the normal enum insertion algorithm cannot find suitable sort order values between existing entries, typically after many enum labels have been added in non-sequential order.

## Parameters / Member Variables
- `pg_enum`: Open relation handle for the pg_enum catalog table
- `*existing`: Array of HeapTuple pointers representing the current enum values in sort order
- `nelems`: Number of elements in the existing array
## Dependencies
- Functions called/Symbols referenced:
  - [heap_copytuple](../h/heap_copytuple.md): Creates writable copies of existing enum tuples
  - Form_pg_enum: Structure type for accessing pg_enum tuple data
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates tuples in the pg_enum catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees temporary tuple memory
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md): Makes updates visible to subsequent operations
- Called from (representative examples):
  - [AddEnumLabel](../A/AddEnumLabel.md): Calls this function when normal insertion cannot find suitable sort order values

## Notes and Other Information
- This is a static function, only accessible within pg_enum.c
- The operation is avoided whenever possible due to concurrency concerns and performance impact
- Works backwards through the array to prevent uniqueness constraint violations
- Only updates tuples whose sort order actually needs to change
- Makes all changes visible with CommandCounterIncrement() after completion
- Critical for maintaining the integrity of enum value ordering when the sort order space becomes fragmented
- The enumsortorder values determine the comparison and sorting behavior of enum values

## Simplified Source

```c
static void
RenumberEnumType(Relation pg_enum, HeapTuple *existing, int nelems)
{
    int i;

    // Renumber existing enum elements to have sort positions 1..n
    // Work backwards to avoid uniqueness violations during update
    for (i = nelems - 1; i >= 0; i--)
    {
        HeapTuple newtup;
        Form_pg_enum en;
        float4 newsortorder;

        newtup = heap_copytuple(existing[i]);
        en = (Form_pg_enum) GETSTRUCT(newtup);

        newsortorder = i + 1;
        if (en->enumsortorder != newsortorder)
        {
            // Update sort order only if it needs to change
            en->enumsortorder = newsortorder;
            CatalogTupleUpdate(pg_enum, &newtup->t_self, newtup);
        }

        heap_freetuple(newtup);
    }

    // Make all updates visible to subsequent operations
    CommandCounterIncrement();
}
```