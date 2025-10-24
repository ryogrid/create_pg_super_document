# getOwnedSeqs

## Location
[src/bin/pg_dump/pg_dump.c:7252-7316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7252-L7316)

## Overview
The getOwnedSeqs function identifies sequences that are owned by table columns and marks them as dumpable if their owning table is being dumped, handling special cases for identity sequences versus regular owned sequences.

## Definition

```c
void
getOwnedSeqs(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
This function processes the relationship between sequences and their owning tables, implementing different dump component inheritance strategies based on sequence type. It was moved out of getTables() to execute after the table lookup index is established, allowing efficient use of findTableByOid() for locating owning tables.

The function distinguishes between two types of owned sequences: identity sequences and other owned sequences (such as serial sequences). For identity sequences, which are considered integral parts of their owning tables and cannot be created independently, the function copies the exact same dump components from the owning table. For other owned sequences, it combines the owning table's dump components with any components explicitly marked for the sequence itself.

This approach handles complex scenarios where a table might be part of an extension (with only non-extension components like ACLs being dumped) while its sequence is not part of the extension (requiring full definition dump). The function also ensures that both sequence and owning table are marked as "interesting" when dump components are present.

## Parameters / Member Variables
- `*fout`: Archive structure containing connection information and dump configuration
- `tblinfo[]`: Array of TableInfo structures representing all tables and sequences
- `numTables`: Number of entries in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid
  - [findTableByOid](../f/findTableByOid.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Executed after getTables() to ensure table lookup index is available for findTableByOid() calls
- Distinguishes between identity sequences (integral to table) and regular owned sequences (like serial)
- Identity sequences inherit exact dump components from owning table due to their integral nature
- Regular owned sequences combine owning table components with sequence-specific components
- Handles extension membership scenarios where table and sequence have different extension status
- Marks both sequence and owning table as "interesting" when dump components are present
- Performs sanity checking to ensure owning table exists for every owned sequence
- Critical for proper dependency handling in dump/restore operations

## Simplified Source

```c
void
getOwnedSeqs(Archive *fout, TableInfo tblinfo[], int numTables)
{
    int i;

    // Process all tables looking for owned sequences
    for (i = 0; i < numTables; i++) {
        TableInfo *seqinfo = &tblinfo[i];
        TableInfo *owning_tab;

        // Skip if not an owned sequence
        if (!OidIsValid(seqinfo->owning_tab))
            continue;

        // Find the owning table
        owning_tab = findTableByOid(seqinfo->owning_tab);
        if (owning_tab == NULL)
            pg_fatal("failed sanity check, parent table with OID %u of sequence with OID %u not found",
                     seqinfo->owning_tab, seqinfo->dobj.catId.oid);

        // Handle dump component inheritance based on sequence type
        if (seqinfo->is_identity_sequence) {
            // Identity sequences are integral to their table - copy exact dump components
            seqinfo->dobj.dump = owning_tab->dobj.dump;
        } else {
            // Regular owned sequences (like serial) - combine table and sequence components
            seqinfo->dobj.dump |= owning_tab->dobj.dump;
        }

        // Mark both sequence and owning table as interesting if dumping any components
        if (seqinfo->dobj.dump != DUMP_COMPONENT_NONE) {
            seqinfo->interesting = true;
            owning_tab->interesting = true;
        }
    }
}
```