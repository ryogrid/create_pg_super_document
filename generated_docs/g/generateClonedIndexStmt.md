# generateClonedIndexStmt

## Location
[src/backend/parser/parse_utilcmd.c:1514-1864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_utilcmd.c#L1514-L1864)

## Overview
Generates an IndexStmt node by cloning the structure and properties of an existing index, adjusting attribute numbers according to a provided mapping for use in table creation scenarios.

## Definition
IndexStmt *generateClonedIndexStmt(RangeVar *heapRel, Relation source_idx, const AttrMap *attmap, Oid *constraintOid)

## Detailed Description
This function creates a complete IndexStmt that recreates an existing index on a different table. It extracts all properties from the source index including access method, uniqueness, primary key status, constraint information, column definitions, expressions, predicates, and options. The function handles both simple column references and complex expression indexes, adjusting all attribute numbers using the provided attribute map. It also processes constraint-related indexes (primary key, unique, exclusion) and copies their constraint properties. The resulting IndexStmt can be executed to create an equivalent index on the target table.

## Parameters / Member Variables
- `heapRel`: RangeVar specifying the target table for the new index (may be NULL if not needed)
- `source_idx`: Relation representing the existing index to clone
- `attmap`: AttrMap for translating attribute numbers from source to target table
- `constraintOid`: Output parameter to store the OID of any associated constraint (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [get_tablespace_name](get_tablespace_name.md)
  - [get_index_constraint](get_index_constraint.md)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - [get_namespace_name](get_namespace_name.md)
  - [stringToNode](../s/stringToNode.md)
  - TextDatumGetCString
  - [map_variable_attnos](../m/map_variable_attnos.md)
  - [get_attname](get_attname.md)
  - [get_atttype](get_atttype.md)
  - [get_collation](get_collation.md)
  - [get_opclass](get_opclass.md)
  - [get_attoptions](get_attoptions.md)
  - [untransformRelOptions](../u/untransformRelOptions.md)
  - [exprType](../e/exprType.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)
  - [expandTableLikeClause](../e/expandTableLikeClause.md)

## Notes and Other Information
- Does not preserve the original index name, allowing DefineIndex to choose a new name
- Rejects whole-row table references in expressions and predicates to prevent future incompatibilities
- Handles both key columns (indnkeyatts) and included columns (indnatts) separately
- Processes exclusion constraints by extracting operator names from pg_constraint
- Supports partial indexes by translating predicate expressions
- Copies index options and column-specific options like collation and operator class
- Sets transformed=true to skip transformIndexStmt processing
- Maintains proper sort ordering and null handling options for ordered access methods

## Simplified Source

```c
IndexStmt *
generateClonedIndexStmt(RangeVar *heapRel, Relation source_idx,
                       const AttrMap *attmap, Oid *constraintOid)
{
    Oid source_relid = RelationGetRelid(source_idx);
    HeapTuple ht_idxrel, ht_idx, ht_am;
    Form_pg_class idxrelrec;
    Form_pg_index idxrec;
    Form_pg_am amrec;
    IndexStmt *index;
    List *indexprs;
    oidvector *indcollation, *indclass;

    if (constraintOid)
        *constraintOid = InvalidOid;

    // Fetch index metadata from system catalogs
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(source_relid));
    idxrelrec = (Form_pg_class) GETSTRUCT(ht_idxrel);

    ht_idx = source_idx->rd_indextuple;
    idxrec = (Form_pg_index) GETSTRUCT(ht_idx);

    ht_am = SearchSysCache1(AMOID, ObjectIdGetDatum(idxrelrec->relam));
    amrec = (Form_pg_am) GETSTRUCT(ht_am);

    // Extract collation and operator class vectors
    indcollation = get_index_collations(ht_idx);
    indclass = get_index_opclasses(ht_idx);

    // Initialize new IndexStmt structure
    index = makeNode(IndexStmt);
    index->relation = heapRel;
    index->accessMethod = pstrdup(NameStr(amrec->amname));

    // Copy tablespace if specified
    if (OidIsValid(idxrelrec->reltablespace))
        index->tableSpace = get_tablespace_name(idxrelrec->reltablespace);

    // Copy index properties
    index->unique = idxrec->indisunique;
    index->nulls_not_distinct = idxrec->indnullsnotdistinct;
    index->primary = idxrec->indisprimary;
    index->transformed = true; // Skip transformIndexStmt

    // Don't preserve original name - let DefineIndex choose
    index->idxname = NULL;

    // Handle constraint-related indexes (PRIMARY, UNIQUE, EXCLUSION)
    if (index->primary || index->unique || idxrec->indisexclusion) {
        Oid constraintId = get_index_constraint(source_relid);

        if (OidIsValid(constraintId)) {
            HeapTuple ht_constr = SearchSysCache1(CONSTROID,
                                                 ObjectIdGetDatum(constraintId));
            Form_pg_constraint conrec = (Form_pg_constraint) GETSTRUCT(ht_constr);

            if (constraintOid)
                *constraintOid = constraintId;

            index->isconstraint = true;
            index->deferrable = conrec->condeferrable;
            index->initdeferred = conrec->condeferred;

            // Handle exclusion constraint operators
            if (idxrec->indisexclusion) {
                index->excludeOpNames = extract_exclusion_operators(ht_constr);
            }

            ReleaseSysCache(ht_constr);
        }
    }

    // Get index expressions if any
    indexprs = get_index_expressions(ht_idx);

    // Build key columns (regular indexed columns)
    index->indexParams = NIL;
    ListCell *indexpr_item = list_head(indexprs);

    for (int keyno = 0; keyno < idxrec->indnkeyatts; keyno++) {
        IndexElem *iparam = makeNode(IndexElem);
        AttrNumber attnum = idxrec->indkey.values[keyno];

        if (AttributeNumberIsValid(attnum)) {
            // Simple column reference
            char *attname = get_attname(idxrec->indrelid, attnum, false);
            iparam->name = attname;
            iparam->expr = NULL;
        } else {
            // Expression index - adjust attribute numbers using attmap
            Node *indexkey = (Node *) lfirst(indexpr_item);
            bool found_whole_row;

            indexkey = map_variable_attnos(indexkey, 1, 0, attmap,
                                         InvalidOid, &found_whole_row);

            if (found_whole_row)
                ereport(ERROR, ...); // Reject whole-row references

            iparam->name = NULL;
            iparam->expr = indexkey;
            indexpr_item = lnext(indexprs, indexpr_item);
        }

        // Copy column properties
        copy_column_properties(iparam, source_idx, keyno,
                              indcollation, indclass);

        index->indexParams = lappend(index->indexParams, iparam);
    }

    // Build included columns (non-key columns)
    index->indexIncludingParams = NIL;
    for (int keyno = idxrec->indnkeyatts; keyno < idxrec->indnatts; keyno++) {
        IndexElem *iparam = makeNode(IndexElem);
        AttrNumber attnum = idxrec->indkey.values[keyno];

        if (AttributeNumberIsValid(attnum)) {
            char *attname = get_attname(idxrec->indrelid, attnum, false);
            iparam->name = attname;
            iparam->expr = NULL;
        } else {
            ereport(ERROR, ...); // Expressions not supported in included columns
        }

        copy_column_name(iparam, source_idx, keyno);
        index->indexIncludingParams = lappend(index->indexIncludingParams, iparam);
    }

    // Copy index options
    copy_index_options(index, ht_idxrel);

    // Handle partial index predicate
    if (has_predicate(ht_idx)) {
        Node *pred_tree = get_index_predicate(ht_idx);
        bool found_whole_row;

        // Adjust predicate attribute numbers using attmap
        pred_tree = map_variable_attnos(pred_tree, 1, 0, attmap,
                                       InvalidOid, &found_whole_row);

        if (found_whole_row)
            ereport(ERROR, ...); // Reject whole-row references

        index->whereClause = pred_tree;
    }

    // Cleanup
    ReleaseSysCache(ht_idxrel);
    ReleaseSysCache(ht_am);

    return index;
}
```