# ConstructTupleDescriptor

## Location
[src/backend/catalog/index.c:280-491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L280-L491)

## Overview
Builds a complete tuple descriptor for a new index by processing column information, data types, and access method requirements to create the structural metadata needed for index storage.

## Definition

```c
static TupleDesc
ConstructTupleDescriptor(Relation heapRelation,
						 const IndexInfo *indexInfo,
						 const List *indexColNames,
						 Oid accessMethodId,
						 const Oid *collationIds,
						 const Oid *opclassIds)
```
## Detailed Description
This function constructs a TupleDesc (tuple descriptor) for a new index by combining information from the heap relation, index specification, and access method requirements. It processes both simple column references and expression-based columns, handling type information, collations, and operator classes. For simple columns, it copies relevant attributes from the heap relation's tuple descriptor. For expression columns, it determines the result type by evaluating the expression and looking up type information in the system catalog. The function also handles special cases like opclass key type overrides and polymorphic type resolution (ANYELEMENT/ANYARRAY). The resulting tuple descriptor serves as the structural definition for how index tuples will be stored and accessed.

## Parameters / Member Variables
- `heapRelation`: Relation pointer to the base table being indexed
- `*indexInfo`: IndexInfo structure containing index metadata including column numbers and expressions
- `*indexColNames`: List of column names for the index (used for naming index attributes)
- `accessMethodId`: OID of the index access method (btree, hash, etc.)
- `*collationIds`: Array of collation OIDs for each key column
- `*opclassIds`: Array of operator class OIDs for each key column
## Dependencies
- Functions called/Symbols referenced:
  - [CreateTemplateTupleDesc](CreateTemplateTupleDesc.md): Creates the base tuple descriptor structure
  - [GetIndexAmRoutineByAmId](../G/GetIndexAmRoutineByAmId.md): Retrieves access method API structure
  - RelationGetDescr: Gets the heap relation's tuple descriptor
  - RelationGetForm: Gets the heap relation's pg_class form
  - TupleDescAttr: Accesses tuple descriptor attributes
  - [list_head](../l/list_head.md)/lnext: List manipulation for iterating through column names and expressions
  - [exprType](../e/exprType.md)/exprTypmod: Determines type and type modifier of expressions
  - [SearchSysCache1](../S/SearchSysCache1.md): Looks up type and opclass information in system cache
  - [CheckAttributeType](CheckAttributeType.md): Validates that the attribute type is safe for index storage
  - [get_base_element_type](../g/get_base_element_type.md): Handles ANYELEMENT/ANYARRAY type resolution
  - MemSet: Initializes attribute structures
  - [namestrcpy](../n/namestrcpy.md): Copies attribute names
- Called from (representative examples):
  - [index_create](../i/index_create.md): During index creation operations
  - SerializedReindexState: During reindex operations

## Notes and Other Information
- The function handles both key attributes and included (non-key) attributes differently
- Expression columns receive special handling for compression settings (set to invalid)
- Key type overrides from opclass or access method take precedence over natural column types
- Polymorphic type resolution supports ANYELEMENT opclasses with ANYARRAY input types
- The attrelid field is initially set to InvalidOid and corrected later by InitializeAttributeOids()
- Memory management includes proper cleanup of system cache entries and access method routines
- Safety checks prevent invalid column references and ensure type compatibility
- The function is static, indicating it's only used within the same source file

## Simplified Source

```c
static TupleDesc ConstructTupleDescriptor(Relation heapRelation, const IndexInfo *indexInfo,
                                         const List *indexColNames, Oid accessMethodId,
                                         const Oid *collationIds, const Oid *opclassIds)
{
    int numatts = indexInfo->ii_NumIndexAttrs;
    int numkeyatts = indexInfo->ii_NumIndexKeyAttrs;
    IndexAmRoutine *amroutine;
    TupleDesc heapTupDesc, indexTupDesc;
    ListCell *colnames_item = list_head(indexColNames);
    ListCell *indexpr_item = list_head(indexInfo->ii_Expressions);

    // Get access method routine and heap tuple descriptor
    amroutine = GetIndexAmRoutineByAmId(accessMethodId, false);
    heapTupDesc = RelationGetDescr(heapRelation);

    // Create new tuple descriptor
    indexTupDesc = CreateTemplateTupleDesc(numatts);

    // Fill in attributes for each index column
    for (int i = 0; i < numatts; i++)
    {
        AttrNumber atnum = indexInfo->ii_IndexAttrNumbers[i];
        Form_pg_attribute to = TupleDescAttr(indexTupDesc, i);
        Oid keyType;

        // Initialize attribute structure
        MemSet(to, 0, ATTRIBUTE_FIXED_PART_SIZE);
        to->attnum = i + 1;
        to->attcacheoff = -1;
        to->attislocal = true;
        to->attcollation = (i < numkeyatts) ? collationIds[i] : InvalidOid;

        // Set attribute name
        namestrcpy(&to->attname, (const char *) lfirst(colnames_item));
        colnames_item = lnext(indexColNames, colnames_item);

        if (atnum != 0)
        {
            // Simple column reference - copy from heap relation
            const FormData_pg_attribute *from = TupleDescAttr(heapTupDesc, AttrNumberGetAttrOffset(atnum));
            to->atttypid = from->atttypid;
            to->attlen = from->attlen;
            to->attndims = from->attndims;
            to->atttypmod = from->atttypmod;
            to->attbyval = from->attbyval;
            to->attalign = from->attalign;
            to->attstorage = from->attstorage;
            to->attcompression = from->attcompression;
        }
        else
        {
            // Expression column - determine type from expression
            Node *indexkey = (Node *) lfirst(indexpr_item);
            indexpr_item = lnext(indexInfo->ii_Expressions, indexpr_item);

            keyType = exprType(indexkey);
            HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
            if (!HeapTupleIsValid(typeTuple))
                elog(ERROR, "cache lookup failed for type %u", keyType);

            Form_pg_type typeTup = (Form_pg_type) GETSTRUCT(typeTuple);
            to->atttypid = keyType;
            to->attlen = typeTup->typlen;
            to->atttypmod = exprTypmod(indexkey);
            to->attbyval = typeTup->typbyval;
            to->attalign = typeTup->typalign;
            to->attstorage = typeTup->typstorage;
            to->attcompression = InvalidCompressionMethod;

            ReleaseSysCache(typeTuple);
            CheckAttributeType(NameStr(to->attname), to->atttypid, to->attcollation, NIL, 0);
        }

        // Handle opclass key type overrides
        keyType = amroutine->amkeytype;
        if (i < indexInfo->ii_NumIndexKeyAttrs)
        {
            // Check if opclass specifies different key type
            HeapTuple opclassTuple = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassIds[i]));
            if (HeapTupleIsValid(opclassTuple))
            {
                Form_pg_opclass opclassTup = (Form_pg_opclass) GETSTRUCT(opclassTuple);
                if (OidIsValid(opclassTup->opckeytype))
                    keyType = opclassTup->opckeytype;

                // Handle ANYELEMENT/ANYARRAY polymorphic types
                if (keyType == ANYELEMENTOID && opclassTup->opcintype == ANYARRAYOID)
                    keyType = get_base_element_type(to->atttypid);

                ReleaseSysCache(opclassTuple);
            }
        }

        // Update type information if key type differs
        if (OidIsValid(keyType) && keyType != to->atttypid)
        {
            HeapTuple typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(keyType));
            Form_pg_type typeTup = (Form_pg_type) GETSTRUCT(typeTuple);

            to->atttypid = keyType;
            to->atttypmod = -1;
            to->attlen = typeTup->typlen;
            to->attbyval = typeTup->typbyval;
            to->attalign = typeTup->typalign;
            to->attstorage = typeTup->typstorage;
            to->attcompression = InvalidCompressionMethod;

            ReleaseSysCache(typeTuple);
        }

        to->attrelid = InvalidOid;  // Fixed later by InitializeAttributeOids
    }

    pfree(amroutine);
    return indexTupDesc;
}
```