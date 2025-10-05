# _bt_allequalimage

## Location
[src/backend/access/nbtree/nbtutils.c:5141-5184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L5141-L5184)

## Overview
Determines whether all attributes in a B-tree index support "equality is image equality" semantics, which is a prerequisite for enabling deduplication optimizations.

## Definition
```c
bool _bt_allequalimage(Relation rel, bool debugmessage)
```

## Detailed Description
This function evaluates whether an index can safely use deduplication by checking if all key attributes have opclasses that support bitwise equality comparisons. It examines each key attribute's BTEQUALIMAGE_PROC opclass procedure to determine if two equal datums are guaranteed to have identical binary representations.

The function iterates through all key attributes, retrieving the appropriate BTEQUALIMAGE_PROC procedure for each attribute's operator family and input type. If any attribute lacks this procedure or the procedure returns false, deduplication is deemed unsafe for the entire index.

The result is typically stored in the index metapage during index builds and determines whether deduplication optimizations can be enabled for the index throughout its lifetime.

## Parameters / Member Variables
- `rel`: The index relation to evaluate for deduplication compatibility
- `debugmessage`: Whether to emit DEBUG1 messages indicating the deduplication capability result

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfAttributes
  - IndexRelationGetNumberOfKeyAttributes
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - [OidFunctionCall1Coll](../O/OidFunctionCall1Coll.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - OidIsValid
  - RelationGetRelationName
  - elog (for debug messages)
  - Constants: BTEQUALIMAGE_PROC, DEBUG1
- Called from (representative examples):
  - [btbuildempty](btbuildempty.md)
  - [_bt_leafbuild](_bt_leafbuild.md)

## Notes and Other Information
- INCLUDE indexes can never support deduplication (non-key attributes prevent it)
- Returns false immediately if the index has non-key (INCLUDE) attributes
- Each attribute must have a valid BTEQUALIMAGE_PROC that returns true
- Missing BTEQUALIMAGE_PROC procedures are treated as unsafe for deduplication
- The function uses the attribute's collation when calling the equality image procedure
- Debug messages clearly indicate whether an index can use deduplication
- [Result](../R/Result.md) affects index metapage settings and runtime deduplication behavior
- Deduplication safety is an all-or-nothing property - all attributes must be compatible
- Essential for determining posting list tuple support and other deduplication optimizations

## Simplified Source

```c
bool _bt_allequalimage(Relation rel, bool debugmessage)
{
    bool allequalimage = true;

    // INCLUDE indexes cannot support deduplication
    if (IndexRelationGetNumberOfAttributes(rel) !=
        IndexRelationGetNumberOfKeyAttributes(rel))
        return false;

    // Check each key attribute for equality image support
    for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(rel); i++)
    {
        Oid opfamily = rel->rd_opfamily[i];
        Oid opcintype = rel->rd_opcintype[i];
        Oid collation = rel->rd_indcollation[i];
        Oid equalimageproc;

        // Get the BTEQUALIMAGE_PROC for this attribute
        equalimageproc = get_opfamily_proc(opfamily, opcintype, opcintype,
                                          BTEQUALIMAGE_PROC);

        // If no procedure exists or it returns false, deduplication is unsafe
        if (!OidIsValid(equalimageproc) ||
            !DatumGetBool(OidFunctionCall1Coll(equalimageproc, collation,
                                              ObjectIdGetDatum(opcintype))))
        {
            allequalimage = false;
            break;
        }
    }

    // Log the result if debug messages are enabled
    if (debugmessage)
    {
        if (allequalimage)
            elog(DEBUG1, "index \"%s\" can safely use deduplication",
                 RelationGetRelationName(rel));
        else
            elog(DEBUG1, "index \"%s\" cannot use deduplication",
                 RelationGetRelationName(rel));
    }

    return allequalimage;
}
```