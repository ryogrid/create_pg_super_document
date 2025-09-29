# ComputeIndexAttrs

## Location
[src/backend/commands/indexcmds.c:1819-2192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/indexcmds.c#L1819-L2192)

## Overview
Computes per-index-column information including indexed column numbers, expressions, operator classes, and their options for all columns in an index definition.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(opclass, &schemaname, &opcname);
```
## Detailed Description
This function is a core component of index creation that processes the attribute list specification and translates it into the internal representation needed by PostgreSQL's index subsystem. It handles both simple column references and complex index expressions, validates data types and collations, resolves operator classes, and sets up exclusion operators for exclusion constraints. The function also handles included columns (non-key columns that are stored but not indexed) and validates various constraints specific to different access methods.

For each column/expression in the index:
1. Determines if it's a simple column reference or an expression
2. Validates the column exists (for simple columns) or expression is valid
3. Extracts type information and collation requirements
4. Resolves the appropriate operator class using ResolveOpClass
5. Sets up exclusion operators if this is an exclusion constraint
6. Configures column options like sort order and null handling
7. Handles security context switching for DDL operations

## Parameters / Member Variables
- : IndexInfo structure to populate with computed information
- : Output array of data type OIDs for each index column
- : Output array of collation OIDs for each index column
- : Output array of operator class OIDs for each index column
- : Output array of operator class options for each index column
- : Output array of column options (sort order, null handling) for each index column
- : Input list of IndexElem structures specifying the index columns/expressions
- : List of exclusion operator names (for exclusion constraints)
- : OID of the relation being indexed
- : Name of the index access method (btree, hash, etc.)
- : OID of the index access method
- : Whether the access method supports ordered indexes
- : Whether this index is being created for a constraint
- : User ID for DDL permission checks (InvalidOid if not needed)
- : Security context for DDL operations
- : Pointer to saved GUC nesting level for DDL operations

## Dependencies
- Functions called/Symbols referenced:
  - [ResolveOpClass](../R/ResolveOpClass.md) (for operator class resolution)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md) (for column lookup)
  - [exprType](../e/exprType.md), exprCollation (for expression analysis)
  - [get_collation_oid](../g/get_collation_oid.md) (for collation resolution)
  - [compatible_oper_opid](../c/compatible_oper_opid.md) (for exclusion operator lookup)
  - [contain_mutable_functions_after_planning](../c/contain_mutable_functions_after_planning.md) (for expression validation)
  - [type_is_collatable](../t/type_is_collatable.md) (for collation validation)
  - [transformRelOptions](../t/transformRelOptions.md) (for operator class options)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (main index creation function)
  - [CheckIndexCompatible](CheckIndexCompatible.md) (index compatibility checking)

## Notes and Other Information
- Handles both key columns and included columns, with different validation rules
- Validates that expressions in included columns are not allowed
- Ensures exclusion operators are commutative and belong to the correct operator family
- Performs security context switching to handle DDL permissions properly
- Supports both simple column references and complex expressions as index keys
- Validates that mutable functions are not used in index expressions
- Sets up proper null ordering defaults based on sort direction for ordered access methods

## Simplified Source

```c
static void ComputeIndexAttrs(IndexInfo *indexInfo,
                             Oid *typeOids, Oid *collationOids, Oid *opclassOids,
                             Datum *opclassOptions, int16 *colOptions,
                             const List *attList, const List *exclusionOpNames,
                             Oid relId, const char *accessMethodName,
                             Oid accessMethodId, bool amcanorder, bool isconstraint,
                             Oid ddl_userid, int ddl_sec_context, int *ddl_save_nestlevel) {

    ListCell *nextExclOp = NULL;
    int nkeycols = indexInfo->ii_NumIndexKeyAttrs;
    Oid save_userid;
    int save_sec_context;

    // Set up exclusion constraint data structures if needed
    if (exclusionOpNames) {
        indexInfo->ii_ExclusionOps = palloc_array(Oid, nkeycols);
        indexInfo->ii_ExclusionProcs = palloc_array(Oid, nkeycols);
        indexInfo->ii_ExclusionStrats = palloc_array(uint16, nkeycols);
        nextExclOp = list_head(exclusionOpNames);
    }

    // Save current security context if needed
    if (OidIsValid(ddl_userid))
        GetUserIdAndSecContext(&save_userid, &save_sec_context);

    // Process each attribute in the index definition
    int attn = 0;
    ListCell *lc;
    foreach(lc, attList) {
        IndexElem *attribute = (IndexElem *) lfirst(lc);
        Oid atttype, attcollation;

        // Handle simple column reference vs expression
        if (attribute->name != NULL) {
            // Simple column - look up in system catalog
            HeapTuple atttuple = SearchSysCacheAttName(relId, attribute->name);
            if (!HeapTupleIsValid(atttuple)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                               errmsg("column \"%s\" does not exist", attribute->name)));
            }

            Form_pg_attribute attform = (Form_pg_attribute) GETSTRUCT(atttuple);
            indexInfo->ii_IndexAttrNumbers[attn] = attform->attnum;
            atttype = attform->atttypid;
            attcollation = attform->attcollation;
            ReleaseSysCache(atttuple);
        }
        else {
            // Index expression
            Node *expr = attribute->expr;

            if (attn >= nkeycols) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("expressions are not supported in included columns")));
            }

            atttype = exprType(expr);
            attcollation = exprCollation(expr);

            // Strip COLLATE clauses and check for simple variable references
            while (IsA(expr, CollateExpr))
                expr = (Node *) ((CollateExpr *) expr)->arg;

            if (IsA(expr, Var) && ((Var *) expr)->varattno != InvalidAttrNumber) {
                // Treat "(column)" as simple attribute
                indexInfo->ii_IndexAttrNumbers[attn] = ((Var *) expr)->varattno;
            }
            else {
                // Real expression - validate and store
                indexInfo->ii_IndexAttrNumbers[attn] = 0;  // Mark as expression
                indexInfo->ii_Expressions = lappend(indexInfo->ii_Expressions, expr);

                // Validate no mutable functions
                if (contain_mutable_functions_after_planning((Expr *) expr)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                                   errmsg("functions in index expression must be marked IMMUTABLE")));
                }
            }
        }

        typeOids[attn] = atttype;

        // Handle included columns (non-key columns)
        if (attn >= nkeycols) {
            // Included columns have restrictions
            if (attribute->collation || attribute->opclass ||
                attribute->ordering != SORTBY_DEFAULT ||
                attribute->nulls_ordering != SORTBY_NULLS_DEFAULT) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                               errmsg("including columns do not support collation, operator class, or ordering options")));
            }

            opclassOids[attn] = InvalidOid;
            opclassOptions[attn] = (Datum) 0;
            colOptions[attn] = 0;
            collationOids[attn] = InvalidOid;
            attn++;
            continue;
        }

        // Handle collation override with security context switching
        if (attribute->collation) {
            if (OidIsValid(ddl_userid)) {
                AtEOXact_GUC(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            attcollation = get_collation_oid(attribute->collation, false);
            if (OidIsValid(ddl_userid)) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                RestrictSearchPath();
            }
        }

        // Validate collation requirements
        if (type_is_collatable(atttype)) {
            if (!OidIsValid(attcollation)) {
                ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_COLLATION),
                               errmsg("could not determine collation for index expression")));
            }
        }
        else if (OidIsValid(attcollation)) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("collations are not supported by type %s",
                                  format_type_be(atttype))));
        }

        collationOids[attn] = attcollation;

        // Resolve operator class with security context switching
        if (OidIsValid(ddl_userid)) {
            AtEOXact_GUC(false, *ddl_save_nestlevel);
            SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
        }
        opclassOids[attn] = ResolveOpClass(attribute->opclass, atttype,
                                          accessMethodName, accessMethodId);
        if (OidIsValid(ddl_userid)) {
            SetUserIdAndSecContext(save_userid, save_sec_context);
            *ddl_save_nestlevel = NewGUCNestLevel();
            RestrictSearchPath();
        }

        // Handle exclusion constraint operators
        if (nextExclOp) {
            List *opname = (List *) lfirst(nextExclOp);

            // Find and validate exclusion operator
            if (OidIsValid(ddl_userid)) {
                AtEOXact_GUC(false, *ddl_save_nestlevel);
                SetUserIdAndSecContext(ddl_userid, ddl_sec_context);
            }
            Oid opid = compatible_oper_opid(opname, atttype, atttype, false);
            if (OidIsValid(ddl_userid)) {
                SetUserIdAndSecContext(save_userid, save_sec_context);
                *ddl_save_nestlevel = NewGUCNestLevel();
                RestrictSearchPath();
            }

            // Validate operator is commutative and in correct family
            if (get_commutator(opid) != opid) {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                               errmsg("operator %s is not commutative", format_operator(opid))));
            }

            Oid opfamily = get_opclass_family(opclassOids[attn]);
            int strat = get_op_opfamily_strategy(opid, opfamily);
            if (strat == 0) {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                               errmsg("operator %s is not a member of the operator family",
                                      format_operator(opid))));
            }

            indexInfo->ii_ExclusionOps[attn] = opid;
            indexInfo->ii_ExclusionProcs[attn] = get_opcode(opid);
            indexInfo->ii_ExclusionStrats[attn] = strat;
            nextExclOp = lnext(exclusionOpNames, nextExclOp);
        }

        // Set up column options (ordering, nulls handling)
        colOptions[attn] = 0;
        if (amcanorder) {
            if (attribute->ordering == SORTBY_DESC)
                colOptions[attn] |= INDOPTION_DESC;

            // Default null ordering: LAST for ASC, FIRST for DESC
            if (attribute->nulls_ordering == SORTBY_NULLS_DEFAULT) {
                if (attribute->ordering == SORTBY_DESC)
                    colOptions[attn] |= INDOPTION_NULLS_FIRST;
            }
            else if (attribute->nulls_ordering == SORTBY_NULLS_FIRST) {
                colOptions[attn] |= INDOPTION_NULLS_FIRST;
            }
        }
        else {
            // Validate no ordering options for non-ordered access methods
            if (attribute->ordering != SORTBY_DEFAULT ||
                attribute->nulls_ordering != SORTBY_NULLS_DEFAULT) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                               errmsg("access method \"%s\" does not support ordering options",
                                      accessMethodName)));
            }
        }

        // Handle operator class options
        if (attribute->opclassopts) {
            opclassOptions[attn] = transformRelOptions((Datum) 0, attribute->opclassopts,
                                                      NULL, NULL, false, false);
        }
        else {
            opclassOptions[attn] = (Datum) 0;
        }

        attn++;
    }
}
```