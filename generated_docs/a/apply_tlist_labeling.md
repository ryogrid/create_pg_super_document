# apply_tlist_labeling

## Location
[src/backend/optimizer/util/tlist.c:318-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L318-L344)

## Overview
Applies the labeling attributes from a source target list to a destination target list, transferring column names and metadata.

## Definition

```c
void
apply_tlist_labeling(List *dest_tlist, List *src_tlist)
```
## Detailed Description
This function transfers the labeling metadata from one target list to another target list that contains equivalent expressions but potentially lacks proper labeling. It's particularly useful for reattaching column names and other metadata to a plan's final output target list after optimizations that may have stripped or altered the labeling information.

The function copies all the labeling attributes that don't affect the actual computed values: resname (column name), ressortgroupref (sort/group reference), resorigtbl (original table OID), resorigcol (original column number), and resjunk (junk column flag). These attributes are essential for proper result formatting and client communication, even though they don't change the underlying data computation.

Both target lists must have the same length and corresponding entries must have matching resno values, which is enforced by assertions.

## Parameters / Member Variables
- `*dest_tlist`: The destination target list that will receive the labeling attributes
- `*src_tlist`: The source target list containing the desired labeling attributes
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (implicit via Assert)
  - forboth (macro for iterating over two lists simultaneously)
  - Assert (debugging assertion macro)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
- Called from (representative examples):
  - [create_plan](../c/create_plan.md)
  - [create_modifytable_plan](../c/create_modifytable_plan.md)
  - [clean_up_removed_plan_level](../c/clean_up_removed_plan_level.md)

## Notes and Other Information
- Both target lists must have identical length (enforced by Assert)
- Corresponding TargetEntry elements must have matching resno values
- Only copies labeling metadata, not the actual expressions
- Essential for maintaining proper column names in query results
- Used during plan finalization to ensure output has correct metadata
- The function assumes the expressions in both lists are equivalent, as verified by other functions like tlist_same_exprs

## Simplified Source

```c
void apply_tlist_labeling(List *dest_tlist, List *src_tlist)
{
    // Verify lists have same length
    Assert(list_length(dest_tlist) == list_length(src_tlist));

    // Iterate through both lists simultaneously
    ListCell *dest_cell, *src_cell;
    forboth(dest_cell, dest_tlist, src_cell, src_tlist)
    {
        TargetEntry *dest_entry = (TargetEntry *) lfirst(dest_cell);
        TargetEntry *src_entry = (TargetEntry *) lfirst(src_cell);

        // Verify corresponding entries have same position number
        Assert(dest_entry->resno == src_entry->resno);

        // Copy all labeling attributes from source to destination
        dest_entry->resname = src_entry->resname;           // Column name
        dest_entry->ressortgroupref = src_entry->ressortgroupref; // Sort/group ref
        dest_entry->resorigtbl = src_entry->resorigtbl;     // Original table OID
        dest_entry->resorigcol = src_entry->resorigcol;     // Original column number
        dest_entry->resjunk = src_entry->resjunk;           // Junk column flag
    }
}
```

This function transfers metadata (names, references, origin info) from one target list to another without changing the actual expressions. Essential for maintaining proper column labeling in query results.