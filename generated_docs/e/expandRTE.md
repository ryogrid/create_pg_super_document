# expandRTE

## Location
[src/backend/parser/parse_relation.c:2659-3016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2659-L3016)

## Overview
Expands the columns of a Range Table Entry (RTE) by creating lists of column names and corresponding Var nodes for each accessible column, handling different RTE types with specific expansion logic.

## Definition

```c
void
expandRTE(RangeTblEntry *rte, int rtindex, int sublevels_up,
		  int location, bool include_dropped,
		  List **colnames, List **colvars)
```
## Detailed Description
This function is a central component of PostgreSQL's query processing system that expands RTE columns into usable column name lists and Var node lists. It handles multiple types of RTEs with type-specific expansion logic:

1. **RTE_RELATION**: Regular table relations - delegates to expandRelation()
2. **RTE_SUBQUERY**: Subquery relations - processes the subquery's target list
3. **RTE_FUNCTION**: Function calls in FROM clause - handles various function return types (scalar, composite, record)
4. **RTE_JOIN**: Join relations - processes join alias variables and column names
5. **RTE_TABLEFUNC/RTE_VALUES/RTE_CTE/RTE_NAMEDTUPLESTORE**: Special table constructs - uses stored column type information
6. **RTE_RESULT**: Result relations - exposes no columns

The function provides flexibility in output by allowing callers to request only column names, only Var nodes, or both. It also handles dropped columns based on the include_dropped parameter, either omitting them or including them as empty strings/NULL constants.

## Parameters / Member Variables
- `*rte`: The Range Table Entry to expand, containing relation information and metadata
- `rtindex`: The range table index (varno) to use in created Var nodes, typically matching the RTE's position
- `sublevels_up`: The varlevelsup value for created Var nodes, indicating nesting level in subqueries
- `location`: Source location information to attach to created Var nodes for error reporting
- `include_dropped`: Boolean flag determining whether to include dropped columns (as empty strings/NULL constants) or omit them
- `**colnames`: Output parameter for list of column name strings (pass NULL if not needed)
- `**colvars`: Output parameter for list of Var nodes representing columns (pass NULL if not needed)
## Dependencies
- Functions called/Symbols referenced:
  - [expandRelation](expandRelation.md) (for RTE_RELATION)
  - [expandTupleDesc](expandTupleDesc.md) (for composite function types)
  - [makeVar](../m/makeVar.md) (creating Var nodes)
  - [makeString](../m/makeString.md) (creating string nodes)
  - [makeNullConst](../m/makeNullConst.md) (creating null constants for dropped columns)
  - [get_expr_result_type](../g/get_expr_result_type.md) (determining function return types)
  - [exprType](exprType.md), exprTypmod, exprCollation (extracting expression type information)
- Data structures used:
  - [RangeTblEntry](../R/RangeTblEntry.md), RangeTblFunction, TargetEntry
  - Various PostgreSQL list manipulation functions
- Called from (representative examples):
  - [transformWholeRowRef](../t/transformWholeRowRef.md) (expanding whole-row references)
  - [expandRecordVariable](expandRecordVariable.md) (expanding record variables)
  - [build_physical_tlist](../b/build_physical_tlist.md) (optimizer planning)
  - [set_relation_column_names](../s/set_relation_column_names.md) (rule utilities)

## Notes and Other Information
- Only user columns are considered; system columns are excluded from expansion
- The function handles complex scenarios like function calls returning composite types, record types, or scalar values
- For JOIN RTEs, the function processes joinaliasvars to handle JOIN USING columns correctly
- Includes extensive error checking, such as verifying subquery target list consistency
- The ordinality column for functions with ORDINALITY is handled as a special case (INT8OID type)
- Critical for query rewriting, planning, and execution phases where column information needs to be materialized
- Part of PostgreSQL's namespace resolution system that translates abstract relation references into concrete column lists

## Simplified Source

```c
void expandRTE(RangeTblEntry *rte, int rtindex, int sublevels_up,
               int location, bool include_dropped,
               List **colnames, List **colvars) {

    // Initialize output lists
    if (colnames) *colnames = NIL;
    if (colvars) *colvars = NIL;

    switch (rte->rtekind) {
        case RTE_RELATION:
            // Regular table - delegate to expandRelation
            expandRelation(rte->relid, rte->eref, rtindex, sublevels_up,
                          location, include_dropped, colnames, colvars);
            break;

        case RTE_SUBQUERY:
            // Subquery - process target list entries
            varattno = 0;
            foreach(item, rte->subquery->targetList) {
                TargetEntry *te = (TargetEntry *) lfirst(item);
                if (te->resjunk) continue;

                varattno++;

                // Add column name if requested
                if (colnames) {
                    char *label = get_column_alias(rte->eref, varattno);
                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                }

                // Add variable if requested
                if (colvars) {
                    Var *varnode = makeVar(rtindex, varattno,
                                         exprType(te->expr),
                                         exprTypmod(te->expr),
                                         exprCollation(te->expr),
                                         sublevels_up);
                    varnode->location = location;
                    *colvars = lappend(*colvars, varnode);
                }
            }
            break;

        case RTE_FUNCTION:
            // Function call - handle different return types
            foreach(func_item, rte->functions) {
                RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(func_item);

                // Determine function return type
                TypeFuncClass functype = get_expr_result_type(rtfunc->funcexpr,
                                                            &rettype, &tupdesc);

                if (functype == TYPEFUNC_COMPOSITE) {
                    // Composite type - expand tuple descriptor
                    expandTupleDesc(tupdesc, rte->eref, rtfunc->funccolcount,
                                   rtindex, sublevels_up, location,
                                   include_dropped, colnames, colvars);
                } else if (functype == TYPEFUNC_SCALAR) {
                    // Scalar type - single column
                    add_scalar_column(rtindex, rettype, rtfunc->funcexpr,
                                     sublevels_up, location, colnames, colvars);
                } else if (functype == TYPEFUNC_RECORD) {
                    // Record type - use column definitions
                    add_record_columns(rte, rtfunc, rtindex, sublevels_up,
                                      location, colnames, colvars);
                }
            }

            // Handle ordinality column if present
            if (rte->funcordinality) {
                add_ordinality_column(rte, rtindex, sublevels_up,
                                    location, colnames, colvars);
            }
            break;

        case RTE_JOIN:
            // Join - process join alias variables
            varattno = 0;
            forboth(name_item, rte->eref->colnames,
                   var_item, rte->joinaliasvars) {
                Node *aliasvar = (Node *) lfirst(var_item);
                varattno++;

                // Handle dropped columns
                if (aliasvar == NULL) {
                    if (include_dropped) {
                        add_dropped_column(colnames, colvars);
                    }
                    continue;
                }

                // Add column name and variable
                if (colnames) {
                    char *label = strVal(lfirst(name_item));
                    *colnames = lappend(*colnames, makeString(pstrdup(label)));
                }

                if (colvars) {
                    Var *varnode = create_join_var(aliasvar, rtindex, varattno,
                                                  sublevels_up, location);
                    *colvars = lappend(*colvars, varnode);
                }
            }
            break;

        case RTE_TABLEFUNC:
        case RTE_VALUES:
        case RTE_CTE:
        case RTE_NAMEDTUPLESTORE:
            // Special table types - use stored column information
            varattno = 0;
            forthree(type_item, rte->coltypes,
                    mod_item, rte->coltypmods,
                    coll_item, rte->colcollations) {
                Oid coltype = lfirst_oid(type_item);
                varattno++;

                if (OidIsValid(coltype)) {
                    // Valid column
                    add_typed_column(rte, rtindex, varattno, coltype,
                                   lfirst_int(mod_item), lfirst_oid(coll_item),
                                   sublevels_up, location, colnames, colvars);
                } else if (include_dropped) {
                    // Dropped column
                    add_dropped_column(colnames, colvars);
                }
            }
            break;

        case RTE_RESULT:
            // Result RTE exposes no columns
            break;

        default:
            elog(ERROR, "unrecognized RTE kind: %d", (int) rte->rtekind);
    }
}
```