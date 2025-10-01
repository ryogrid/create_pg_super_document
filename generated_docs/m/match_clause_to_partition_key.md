# match_clause_to_partition_key

## Location
[src/backend/partitioning/partprune.c:1790-2437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L1790-L2437)

## Overview
Attempts to match a given clause with a specified partition key and determines how the clause can be used for partition pruning.

## Definition

```c
struct_array(arrval,
							  ARR_ELEMTYPE(arrval),
							  elemlen, elembyval, elemalign,
							  &elem_values, &elem_nulls,
							  &num_elems);
```
## Detailed Description
This function is a core component of PostgreSQL's partition pruning mechanism. It analyzes various types of SQL clauses (WHERE conditions, JOIN conditions, etc.) to determine if they can be used to eliminate irrelevant partitions during query execution. The function supports multiple clause types including:

- Boolean partition clauses (for boolean partition keys)
- Binary operator expressions (=, <, >, <=, >=, <>)
- Scalar array operations (IN, NOT IN, ANY, ALL)
- NULL test expressions (IS NULL, IS NOT NULL)

The function performs extensive validation including operator family membership checks, collation matching, mutability analysis, and parameter detection to ensure the clause is suitable for pruning at the target execution phase (planner vs executor).

## Parameters / Member Variables
- : Context information for generating pruning steps, including target phase and partition relation info
- : The SQL expression/clause to be matched against the partition key
- : The partition key expression to match against
- : Index of the partition key in the partition scheme
- : Output parameter set when clause matches NULL/NOT NULL tests
- : Output parameter set to PartClauseInfo when clause can be directly used for pruning
- : Output parameter set to list of generated pruning steps for complex clauses

## Dependencies
- Functions called/Symbols referenced:
  - [match_boolean_partition_clause](match_boolean_partition_clause.md)
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - PartCollMatchesExprColl
  - [contain_var_clause](../c/contain_var_clause.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [pull_exec_paramids](../p/pull_exec_paramids.md)
- Called from:
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)

## Notes and Other Information
The function returns different PartClauseMatchStatus values indicating the match result:
- PARTCLAUSE_MATCH_CLAUSE: Direct clause match, PartClauseInfo created
- PARTCLAUSE_MATCH_NULLNESS: NULL test match
- PARTCLAUSE_MATCH_STEPS: Complex clause requiring step generation
- PARTCLAUSE_MATCH_CONTRADICT: Self-contradictory clause
- PARTCLAUSE_NOMATCH: No match with this key, try others
- PARTCLAUSE_UNSUPPORTED: Clause form unsuitable for any partition key

Special handling exists for NOT IN operations with list partitioning and boolean partition keys with IS NOT TRUE/IS NOT FALSE tests.

## Simplified Source

```c
static PartClauseMatchStatus match_clause_to_partition_key(GeneratePruningStepsContext *context,
                                                          Expr *clause, Expr *partkey, int partkeyidx,
                                                          bool *clause_is_not_null, PartClauseInfo **pc,
                                                          List **clause_steps) {
    PartitionScheme part_scheme = context->rel->part_scheme;
    Oid partopfamily = part_scheme->partopfamily[partkeyidx];

    // Try boolean partition clause matching first
    PartClauseMatchStatus boolmatchstatus = match_boolean_partition_clause(partopfamily, clause,
                                                                         partkey, &expr, &notclause);
    if (boolmatchstatus == PARTCLAUSE_MATCH_CLAUSE) {
        // Handle boolean operators - create PartClauseInfo
        PartClauseInfo *partclause = palloc(sizeof(PartClauseInfo));
        partclause->keyno = partkeyidx;
        partclause->opno = BooleanEqualOperator;
        partclause->expr = expr;
        partclause->cmpfn = part_scheme->partsupfunc[partkeyidx].fn_oid;
        *pc = partclause;
        return PARTCLAUSE_MATCH_CLAUSE;
    }

    // Handle OpExpr (=, <, >, <=, >=, <>)
    if (IsA(clause, OpExpr) && list_length(((OpExpr *) clause)->args) == 2) {
        OpExpr *opclause = (OpExpr *) clause;
        Expr *leftop = get_leftop(clause);
        Expr *rightop = get_rightop(clause);

        // Match partition key to left or right operand
        if (equal(leftop, partkey)) {
            expr = rightop;
        } else if (equal(rightop, partkey)) {
            // Try to commute the operator
            Oid commutator = get_commutator(opclause->opno);
            if (!OidIsValid(commutator))
                return PARTCLAUSE_UNSUPPORTED;
            expr = leftop;
        } else {
            return PARTCLAUSE_NOMATCH;
        }

        // Validate operator is in partition opfamily and is strict
        if (!op_in_opfamily(opclause->opno, partopfamily) || !op_strict(opclause->opno))
            return PARTCLAUSE_UNSUPPORTED;

        // Validate expression constraints (constants, mutability, etc.)
        if (!validate_expression_constraints(context, expr))
            return PARTCLAUSE_UNSUPPORTED;

        // Create and return PartClauseInfo
        PartClauseInfo *partclause = create_partclause_info(context, partkeyidx,
                                                          opclause->opno, expr);
        *pc = partclause;
        return PARTCLAUSE_MATCH_CLAUSE;
    }

    // Handle ScalarArrayOpExpr (IN, NOT IN)
    if (IsA(clause, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;

        // Check if left operand matches partition key
        if (!equal(linitial(saop->args), partkey))
            return PARTCLAUSE_NOMATCH;

        // Convert array elements to individual clauses and generate steps
        List *elem_clauses = convert_array_to_clauses(saop);
        *clause_steps = gen_partprune_steps_internal(context, elem_clauses);

        if (*clause_steps == NIL)
            return PARTCLAUSE_UNSUPPORTED;
        return PARTCLAUSE_MATCH_STEPS;
    }

    // Handle NullTest (IS NULL, IS NOT NULL)
    if (IsA(clause, NullTest)) {
        NullTest *nulltest = (NullTest *) clause;
        if (!equal(nulltest->arg, partkey))
            return PARTCLAUSE_NOMATCH;

        *clause_is_not_null = (nulltest->nulltesttype == IS_NOT_NULL);
        return PARTCLAUSE_MATCH_NULLNESS;
    }

    return boolmatchstatus;
}
```