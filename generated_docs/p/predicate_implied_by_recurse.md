# predicate_implied_by_recurse

## Location
[src/backend/optimizer/util/predtest.c:290-530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L290-L530)

## Overview
Recursively performs the core logical implication testing between clauses and predicates using comprehensive case analysis of AND/OR expression structures.

## Definition
```c
static bool predicate_implied_by_recurse(Node *clause, Node *predicate, bool weak)
```

## Detailed Description
This function implements the recursive core logic for predicate implication testing. It handles all combinations of AND-expressions, OR-expressions, and atomic expressions using these logical rules:

- **atom A => atom B**: Uses predicate_implied_by_simple_clause for base cases
- **atom A => AND-expr B**: A must imply each of Bs components  
- **atom A => OR-expr B**: A must imply any of Bs components
- **AND-expr A => atom B**: Any of As components must imply B
- **AND-expr A => AND-expr B**: A must imply each of Bs components
- **AND-expr A => OR-expr B**: A must imply any of Bs components, OR any of As components must imply B
- **OR-expr A => atom B**: Each of As components must imply B
- **OR-expr A => AND-expr B**: A must imply each of Bs components
- **OR-expr A => OR-expr B**: Each of As components must imply any of Bs components

The function handles RestrictInfo nodes in the clause tree and uses predicate_classify to determine expression types before applying the appropriate logical rules.

## Parameters / Member Variables
- `clause`: The clause/restriction that is assumed to be true (may contain RestrictInfo nodes)
- `predicate`: The predicate expression to be proven true
- `weak`: Boolean indicating whether to use weak (true) or strong (false) implication semantics

## Dependencies
- Functions called/Symbols referenced:
  - [predicate_classify](predicate_classify.md) (classifies expressions as AND/OR/ATOM)
  - [predicate_implied_by_simple_clause](predicate_implied_by_simple_clause.md) (handles atom => atom base cases)
  - iterate_begin/iterate_end (macros for iterating over AND/OR components)
  - [PredIterInfoData](../P/PredIterInfoData.md) (structure for iteration state)
  - [PredClass](../P/PredClass.md) enumeration (CLASS_AND, CLASS_OR, CLASS_ATOM)
- Called from (representative examples):
  - [predicate_implied_by](predicate_implied_by.md) (top-level entry point)
  - [predicate_implied_by_recurse](predicate_implied_by_recurse.md) (recursive self-calls)
  - [predicate_refuted_by_recurse](predicate_refuted_by_recurse.md) (similar logic for refutation)

## Notes and Other Information
- Static function - internal implementation detail of predtest.c
- Handles complex logical expressions by breaking them down systematically
- Automatically strips RestrictInfo wrappers from clause nodes
- Uses comprehensive case analysis to ensure all logical combinations are covered
- Critical for query optimization decisions involving index predicates and constraints
- The logic applies equally to both strong and weak implication modes
- Designed to work with flattened AND/OR expressions from eval_const_expressions()

## Simplified Source

```c
static bool predicate_implied_by_recurse(Node *clause, Node *predicate, bool weak) {
    // Strip RestrictInfo wrapper if present
    if (IsA(clause, RestrictInfo))
        clause = (Node *) ((RestrictInfo *) clause)->clause;

    // Classify both clause and predicate as AND/OR/ATOM
    PredClass clause_class = predicate_classify(clause, &clause_info);
    PredClass pred_class = predicate_classify(predicate, &pred_info);

    switch (clause_class) {
        case CLASS_AND:
            if (pred_class == CLASS_AND) {
                // AND => AND: clause must imply each predicate component
                foreach_predicate_component(pitem, predicate) {
                    if (!predicate_implied_by_recurse(clause, pitem, weak))
                        return false;
                }
                return true;
            }
            else if (pred_class == CLASS_OR) {
                // AND => OR: clause implies any predicate component OR
                // any clause component implies predicate
                return (clause_implies_any_pred_component(clause, predicate, weak) ||
                        any_clause_component_implies_pred(clause, predicate, weak));
            }
            else { // CLASS_ATOM
                // AND => atom: any clause component must imply predicate
                return any_clause_component_implies_pred(clause, predicate, weak);
            }
            break;

        case CLASS_OR:
            if (pred_class == CLASS_OR) {
                // OR => OR: each clause component must imply some predicate component
                foreach_clause_component(citem, clause) {
                    if (!clause_component_implies_any_pred(citem, predicate, weak))
                        return false;
                }
                return true;
            }
            else { // CLASS_AND or CLASS_ATOM
                // OR => AND/atom: each clause component must imply predicate
                foreach_clause_component(citem, clause) {
                    if (!predicate_implied_by_recurse(citem, predicate, weak))
                        return false;
                }
                return true;
            }
            break;

        case CLASS_ATOM:
            if (pred_class == CLASS_AND) {
                // atom => AND: clause must imply each predicate component
                foreach_predicate_component(pitem, predicate) {
                    if (!predicate_implied_by_recurse(clause, pitem, weak))
                        return false;
                }
                return true;
            }
            else if (pred_class == CLASS_OR) {
                // atom => OR: clause must imply any predicate component
                return clause_implies_any_pred_component(clause, predicate, weak);
            }
            else { // CLASS_ATOM
                // atom => atom: base case
                return predicate_implied_by_simple_clause((Expr *) predicate, clause, weak);
            }
            break;
    }

    return false;
}
```