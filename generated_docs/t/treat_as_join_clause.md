# treat_as_join_clause

## Location
[src/backend/optimizer/path/clausesel.c:586-666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L586-L666)

## Overview
Determines whether an operator clause should be handled by the restriction selectivity estimator or the join selectivity estimator as part of the query optimization process.

## Definition

```c
struct, and only the
 *	   relids and jointype fields in it can be trusted.
 * It is possible for jointype to be different from sjinfo->jointype.
 * This indicates we are considering a variant join: either with
 * the LHS and RHS switched, or with one input unique-ified.
 *
 * Note: when passing nonzero varRelid, it's normally appropriate to set
 * jointype == JOIN_INNER, sjinfo == NULL, even if the clause is really a
 * join clause;
```
## Detailed Description
This function serves as a decision point in PostgreSQL's query optimizer to classify clauses for selectivity estimation. The function implements a hierarchical decision logic:

1. **Forced restriction mode**: When , the caller is explicitly requesting restriction mode (e.g., for inner indexscan qualifiers), so the clause is treated as a restriction clause.

2. **Scan-level evaluation**: When , the clause is being evaluated at a scan node, making it a restriction clause by definition.

3. **Join-level evaluation**: In all other cases, the function determines if the clause involves multiple base relations. If so, it's treated as a join clause; otherwise, it's a restriction clause.

The function includes an optimization for cases where a  structure is available, using the pre-computed  field instead of calling . It specifically counts only base relations, not outer joins, ensuring that clauses delayed by outer joins are directed to restriction estimators rather than join estimators.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer information
- : The Node representing the clause to be classified
- : Optional RestrictInfo structure with pre-computed clause metadata for optimization
- : If non-zero, forces restriction mode regardless of other factors
- : SpecialJoinInfo structure indicating join context; NULL means scan-level evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [NumRelids](../N/NumRelids.md) (when rinfo is not available)
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md) (multiple locations in clausesel.c)

## Notes and Other Information
- The function is marked  for performance optimization
- Contains detailed comments about handling outer join scenarios and the intentional exclusion of outer joins from base relation counting
- The XXX comment indicates potential future enhancement for handling injected nulls from outer joins
- This function is a critical component in PostgreSQL's cost-based optimization, affecting how selectivity estimates are computed for different types of clauses

## Simplified Source

```c
static inline bool
treat_as_join_clause(PlannerInfo *root, Node *clause, RestrictInfo *rinfo,
                     int varRelid, SpecialJoinInfo *sjinfo)
{
    // Forced restriction mode - caller is forcing restriction evaluation
    if (varRelid != 0) {
        return false;
    }

    // Scan-level evaluation - being evaluated at a scan node
    else if (sjinfo == NULL) {
        return false;
    }

    // Join-level evaluation - determine based on number of relations
    else {
        // Use pre-computed value if available, otherwise count relations
        if (rinfo) {
            return (rinfo->num_base_rels > 1);
        } else {
            return (NumRelids(root, clause) > 1);
        }
    }
}
```