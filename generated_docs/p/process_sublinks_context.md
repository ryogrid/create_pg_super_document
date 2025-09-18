# process_sublinks_context

## Location
src/backend/optimizer/plan/subselect.c: 48 - 52

## Overview
A context structure used during the expansion of SubLinks to SubPlans in query expressions, maintaining planner state and tracking whether expressions are at the top-level qualifier level.

## Definition


## Detailed Description
The  structure serves as a parameter context for the  function during the process of expanding SubLinks into SubPlans. This structure is central to PostgreSQL's subquery processing pipeline, where subqueries are transformed from their parsed representation (SubLink nodes) into executable subplans. The context tracks both the current planner state and whether the current expression node is at the top level of a WHERE/HAVING qualifier, which affects how NULL vs FALSE distinctions are handled in subquery evaluation.

## Parameters / Member Variables
- : PlannerInfo pointer containing the current planner state, query context, and optimization information needed during subquery planning
- : Boolean flag indicating whether the current expression is at the top level of a WHERE/HAVING qualifier, which determines whether sublinks need to distinguish between FALSE and UNKNOWN return values

## Dependencies
- Functions called/Symbols referenced:
  - PlannerInfo (structure)
- Called from (representative examples):
  - SS_process_sublinks (src/backend/optimizer/plan/subselect.c:1921)
  - process_sublinks_mutator (src/backend/optimizer/plan/subselect.c:1929)
  - process_sublinks_mutator (src/backend/optimizer/plan/subselect.c:1931)

## Notes and Other Information
- This context structure is part of PostgreSQL's expression tree mutator pattern for subquery processing
- The  flag is critical for optimization: at top-level qualifiers, NULL and FALSE can be treated as equivalent, allowing for certain optimizations
- Used internally during the subquery planning phase to transform parsed SubLink nodes into executable SubPlan nodes
- The context is passed recursively through the expression tree, with the  flag being modified based on the current position in AND/OR clause structures
- Part of the core subselect processing infrastructure that handles the conversion from parser output to executable query plans
- The  semantics differ from other PostgreSQL contexts - here it propagates down through AND/OR clauses rather than being reset at lower levels