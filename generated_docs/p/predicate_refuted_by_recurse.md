# predicate_refuted_by_recurse

## Location
[src/backend/optimizer/util/predtest.c:531-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L531-L825)

## Overview
Recursively performs the core logical refutation testing between clauses and predicates using comprehensive case analysis of AND/OR expression structures and NOT-clause handling.

## Definition
```c
static bool predicate_refuted_by_recurse(Node *clause, Node *predicate, bool weak)
```

## Detailed Description
This function implements the recursive core logic for predicate refutation testing. It handles all combinations of AND-expressions, OR-expressions, and atomic expressions using these logical rules:

- **atom A R=> atom B**: Uses predicate_refuted_by_simple_clause for base cases
- **atom A R=> AND-expr B**: A must refute any of Bs components
- **atom A R=> OR-expr B**: A must refute each of Bs components  
- **AND-expr A R=> atom B**: Any of As components must refute B
- **AND-expr A R=> AND-expr B**: A must refute any of Bs components, OR any of As components must refute B
- **AND-expr A R=> OR-expr B**: A must refute each of Bs components
- **OR-expr A R=> atom B**: Each of As components must refute B
- **OR-expr A R=> AND-expr B**: Each of As components must refute any of Bs components
- **OR-expr A R=> OR-expr B**: A must refute each of Bs components

The function includes special handling for NOT-clauses:
- **A R=> NOT B** if A implies B
- **NOT A R=> B** if B implies A (with appropriate strong/weak handling)

## Parameters / Member Variables
- `clause`: The clause/restriction that is assumed to be true (may contain RestrictInfo nodes)
- `predicate`: The predicate expression to be disproven (shown false)
- `weak`: Boolean indicating whether to use weak (true) or strong (false) refutation semantics

## Dependencies
- Functions called/Symbols referenced:
  - [predicate_classify](predicate_classify.md) (classifies expressions as AND/OR/ATOM)
  - [predicate_refuted_by_simple_clause](predicate_refuted_by_simple_clause.md) (handles atom R=> atom base cases)
  - [predicate_implied_by_recurse](predicate_implied_by_recurse.md) (used for NOT-clause handling)
  - [extract_not_arg](../e/extract_not_arg.md) (extracts argument from NOT-type clauses)
  - [extract_strong_not_arg](../e/extract_strong_not_arg.md) (extracts argument from strong NOT clauses)
  - iterate_begin/iterate_end (macros for iterating over AND/OR components)
  - [PredIterInfoData](../P/PredIterInfoData.md) (structure for iteration state)
  - [PredClass](../P/PredClass.md) enumeration (CLASS_AND, CLASS_OR, CLASS_ATOM)
- Called from (representative examples):
  - [predicate_refuted_by](predicate_refuted_by.md) (top-level entry point)
  - [predicate_refuted_by_recurse](predicate_refuted_by_recurse.md) (recursive self-calls)

## Notes and Other Information
- Static function - internal implementation detail of predtest.c
- Handles complex logical expressions by breaking them down systematically with refutation rules
- Automatically strips RestrictInfo wrappers from clause nodes
- Special logic for NOT-clauses leverages implication testing for refutation proofs
- Uses comprehensive case analysis to ensure all logical combinations are covered
- Critical for query optimization in constraint-based table exclusion and partition pruning
- Strong NOT-clause handling allows proving refutation when the NOTs argument is false
- The logic applies equally to both strong and weak refutation modes
- Designed to work with flattened AND/OR expressions from eval_const_expressions()

## Simplified Source
```c
static bool
predicate_refuted_by_recurse(Node *clause, Node *predicate, bool weak)
{
    PredClass clause_class, pred_class;
    Node *not_arg;
    bool result;

    // Strip RestrictInfo wrapper if present
    if (IsA(clause, RestrictInfo))
        clause = ((RestrictInfo *) clause)->clause;

    // Classify both expressions as AND/OR/ATOM
    clause_class = predicate_classify(clause, &clause_info);
    pred_class = predicate_classify(predicate, &pred_info);

    switch (clause_class) {
        case CLASS_AND:
            switch (pred_class) {
                case CLASS_AND:
                    // AND R=> AND: clause refutes any predicate component
                    // OR any clause component refutes predicate
                    return check_any_refutes_any(clause, predicate, weak);

                case CLASS_OR:
                    // AND R=> OR: clause must refute each predicate component
                    return check_refutes_all(clause, predicate, weak);

                case CLASS_ATOM:
                    // Handle NOT-clauses specially
                    not_arg = extract_not_arg(predicate);
                    if (not_arg && predicate_implied_by_recurse(clause, not_arg, false))
                        return true;
                    // AND R=> atom: any clause component refutes predicate
                    return check_any_component_refutes(clause, predicate, weak);
            }

        case CLASS_OR:
            switch (pred_class) {
                case CLASS_OR:
                    // OR R=> OR: clause must refute each predicate component
                    return check_refutes_all(clause, predicate, weak);

                case CLASS_AND:
                    // OR R=> AND: each clause component must refute some predicate component
                    return check_each_refutes_any(clause, predicate, weak);

                case CLASS_ATOM:
                    // Handle NOT-clauses
                    not_arg = extract_not_arg(predicate);
                    if (not_arg && predicate_implied_by_recurse(clause, not_arg, false))
                        return true;
                    // OR R=> atom: each clause component must refute predicate
                    return check_all_refute(clause, predicate, weak);
            }

        case CLASS_ATOM:
            // Handle strong NOT-clauses in clause
            not_arg = extract_strong_not_arg(clause);
            if (not_arg && predicate_implied_by_recurse(predicate, not_arg, !weak))
                return true;

            switch (pred_class) {
                case CLASS_AND:
                    // atom R=> AND: refute any predicate component
                    return check_refutes_any(clause, predicate, weak);

                case CLASS_OR:
                    // atom R=> OR: refute each predicate component
                    return check_refutes_all(clause, predicate, weak);

                case CLASS_ATOM:
                    // Handle NOT-clauses in predicate
                    not_arg = extract_not_arg(predicate);
                    if (not_arg && predicate_implied_by_recurse(clause, not_arg, false))
                        return true;
                    // Base case: use simple clause refutation
                    return predicate_refuted_by_simple_clause(predicate, clause, weak);
            }
    }

    return false;
}
```