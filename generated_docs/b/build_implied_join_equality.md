# build_implied_join_equality

## Location
[src/backend/optimizer/plan/initsplan.c:3100-3168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L3100-L3168)

## Overview
Constructs a RestrictInfo representing a derived equality clause for join operations without integrating it into the joininfo tree structure.

## Definition

```c
structure with
	 * original (this is necessary in case there are subselects in there...)
	 */
	clause = make_opclause(opno,
						   BOOLOID, /* opresulttype */
						   false,	/* opretset */
						   copyObject(item1),
						   copyObject(item2),
						   InvalidOid,
						   collation);
```
## Detailed Description
This function creates implied equality clauses specifically for join scenarios where the clause should not be automatically distributed to relation joininfo lists. It's a specialized version of process_implied_equality() that builds the RestrictInfo but skips the distribution phase, giving the caller more control over how and where the clause is used.

The function performs these key operations:
1. **Expression construction**: Builds an OpExpr representing "item1 op item2" 
2. **Deep copying**: Ensures the new clause shares no substructure with the originals
3. **RestrictInfo creation**: Constructs the wrapper with appropriate flags and metadata
4. **Join analysis**: Analyzes suitability for merge joins, hash joins, and memoization

Unlike process_implied_equality(), this function does not perform constant folding, variable-free clause handling, or automatic distribution to relation lists.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and context
- : OID of the operator to use (typically a btree equality operator)
- : Collation OID for the comparison operation
- : Left operand expression (will be deep copied)
- : Right operand expression (will be deep copied)
- : Relids indicating which relations the clause references
- : Security level to assign to the resulting RestrictInfo

## Dependencies
- Functions called/Symbols referenced:
  - make_opclause (creates operator expression node)
  - copyObject (performs deep copying of expression trees)
  - [make_restrictinfo](../m/make_restrictinfo.md) (constructs RestrictInfo wrapper)
  - [check_mergejoinable](../c/check_mergejoinable.md) (analyzes merge join suitability)
  - [check_hashjoinable](../c/check_hashjoinable.md) (analyzes hash join suitability)
  - [check_memoizable](../c/check_memoizable.md) (analyzes memoization potential for nested loops)

- Called from (representative examples):
  - [create_join_clause](../c/create_join_clause.md) (equivalence class join clause creation)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md) (outer join clause reconsideration)
  - [reconsider_full_join_clause](../r/reconsider_full_join_clause.md) (full join clause reconsideration)

## Notes and Other Information
- Does not automatically distribute the clause to joininfo lists (unlike process_implied_equality)
- Caller responsible for equivalence class initialization via initialize_mergeclause_eclasses()
- Always creates non-pseudoconstant RestrictInfo (assumes join context)
- Performs comprehensive join method analysis (merge, hash, memoization)
- Part of PostgreSQL's equivalence class system for advanced join optimization
- Used primarily in contexts where manual clause placement control is needed
- Simpler than process_implied_equality() as it skips distribution complexity