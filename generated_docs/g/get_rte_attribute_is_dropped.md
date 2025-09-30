# get_rte_attribute_is_dropped

## Location
[src/backend/parser/parse_relation.c:3291-3438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3291-L3438)

## Overview
Checks whether an attempted attribute reference is to a dropped column within a range table entry (RTE).

## Definition

```c
structed,
				 * but one in a stored rule might contain columns that were
				 * dropped from the underlying tables, if said columns are
				 * nowhere explicitly referenced in the rule.  This will be
				 * signaled to us by a null pointer in the joinaliasvars list.
				 */
				Var		   *aliasvar;
```
## Detailed Description
This function determines if a specified attribute (column) in a range table entry has been dropped. It handles different types of RTEs with specific logic for each:

- **RTE_RELATION**: Queries the system catalog (pg_attribute) to check the  flag
- **RTE_SUBQUERY/RTE_TABLEFUNC/RTE_VALUES/RTE_CTE**: These never have dropped columns, so always returns false
- **RTE_NAMEDTUPLESTORE**: Checks for dropped columns by testing if the column type is valid
- **RTE_JOIN**: Checks if the joinaliasvars list contains a NULL pointer at the specified position, indicating a dropped column
- **RTE_FUNCTION**: For composite function results, checks the tuple descriptor to see if the column is dropped
- **RTE_RESULT**: Reports an error as this shouldn't normally happen

The function is essential for query planning and execution to avoid referencing columns that no longer exist in the underlying tables.

## Parameters / Member Variables
- : Range table entry to check for dropped attributes
- : Attribute number (column position) to check for dropped status

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) (for catalog lookups)
  - [Int16GetDatum](../I/Int16GetDatum.md) (for datum conversion)
  - [list_nth_oid](../l/list_nth_oid.md) (for list operations)
  - [list_nth](../l/list_nth.md) (for list operations)
  - [get_expr_result_tupdesc](get_expr_result_tupdesc.md) (for function result type analysis)
  - Various RTE kind constants (RTE_RELATION, RTE_SUBQUERY, etc.)
- Called from (representative examples):
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - rt_fetch (via macro expansion)

## Notes and Other Information
- This function is crucial for maintaining data integrity when columns are dropped from tables
- The function handles stored rules that might reference dropped columns by checking for NULL pointers in join alias variable lists
- For function RTEs returning composite types, it performs deeper analysis of the result tuple descriptor
- The function includes comprehensive error handling for invalid attribute numbers and unrecognized RTE kinds

## Simplified Source

```c
bool get_rte_attribute_is_dropped(RangeTblEntry *rte, AttrNumber attnum) {
    switch (rte->rtekind) {
        case RTE_RELATION:
            // Look up attribute in system catalog
            HeapTuple tp = SearchSysCache2(ATTNUM,
                                         ObjectIdGetDatum(rte->relid),
                                         Int16GetDatum(attnum));
            if (!HeapTupleIsValid(tp)) {
                elog(ERROR, "cache lookup failed for attribute %d", attnum);
            }

            Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
            bool result = att_tup->attisdropped;
            ReleaseSysCache(tp);
            return result;

        case RTE_SUBQUERY:
        case RTE_TABLEFUNC:
        case RTE_VALUES:
        case RTE_CTE:
            // These RTE types never have dropped columns
            return false;

        case RTE_NAMEDTUPLESTORE:
            // Check if column type is valid (dropped columns have invalid OID)
            if (attnum <= 0 || attnum > list_length(rte->coltypes)) {
                elog(ERROR, "invalid varattno %d", attnum);
            }
            return !OidIsValid(list_nth_oid(rte->coltypes, attnum - 1));

        case RTE_JOIN:
            // Check if joinaliasvars has NULL pointer (indicates dropped column)
            if (attnum <= 0 || attnum > list_length(rte->joinaliasvars)) {
                elog(ERROR, "invalid varattno %d", attnum);
            }
            Var *aliasvar = (Var *) list_nth(rte->joinaliasvars, attnum - 1);
            return (aliasvar == NULL);

        case RTE_FUNCTION:
            // For functions returning composite types, check tuple descriptor
            int atts_done = 0;
            foreach(lc, rte->functions) {
                RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

                if (attnum > atts_done && attnum <= atts_done + rtfunc->funccolcount) {
                    // If has column definition list, returns RECORD (no dropped columns)
                    if (rtfunc->funccolnames != NIL) {
                        return false;
                    }

                    // Get tuple descriptor and check attribute
                    TupleDesc tupdesc = get_expr_result_tupdesc(rtfunc->funcexpr, true);
                    if (tupdesc) {
                        Form_pg_attribute att_tup = TupleDescAttr(tupdesc, attnum - atts_done - 1);
                        return att_tup->attisdropped;
                    }
                    return false;
                }
                atts_done += rtfunc->funccolcount;
            }

            // Check for ordinality column
            if (rte->funcordinality && attnum == atts_done + 1) {
                return false;
            }

            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                          errmsg("column %d does not exist", attnum)));
            return false;

        case RTE_RESULT:
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                          errmsg("column %d does not exist", attnum)));
            return false;

        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
            return false;
    }
}
```