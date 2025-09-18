# StoreViewQuery

## Location
src/backend/commands/view.c: 511 - 517

## Overview
StoreViewQuery stores the query definition for a view using the PostgreSQL rules system, serving as a simple wrapper around DefineViewRules.

## Definition


## Detailed Description
StoreViewQuery is a straightforward function that encapsulates the process of storing a view's query definition in the PostgreSQL rules system. It serves as an abstraction layer that could potentially be extended in the future to handle additional view-related storage operations beyond just rule creation.

Currently, the function's only operation is to delegate to DefineViewRules, which creates the ON SELECT rule that defines how queries against the view should be rewritten. This design provides a clean separation between the high-level concept of "storing a view query" and the low-level implementation details of rule system interaction.

The function is used in multiple contexts:
- During initial view creation (both new views and view replacement)
- In CREATE TABLE AS operations that involve views
- Any other scenario where a view's query definition needs to be persisted

The simplicity of this function suggests it may be designed as a stable API that can be extended with additional functionality (such as storing view metadata, handling view dependencies, or managing view-related caching) without requiring changes to calling code.

## Parameters / Member Variables
- : Object identifier of the view relation for which the query is being stored
- : Query tree representing the view's SELECT statement, already fully parsed and analyzed
- : Boolean indicating whether existing view rules should be replaced (for CREATE OR REPLACE VIEW scenarios)

## Dependencies
- Functions called/Symbols referenced:
  - [DefineViewRules](../D/DefineViewRules.md)

- Called from:
  - [DefineVirtualRelation](../D/DefineVirtualRelation.md) (during both new view creation and view replacement)
  - [create_ctas_internal](../c/create_ctas_internal.md) (for CREATE TABLE AS operations involving views)

## Notes and Other Information
- This function is designed as a clean abstraction over the rules system interaction
- Its simple implementation suggests it's designed to be extensible for future view storage requirements
- The function name emphasizes the conceptual operation (storing a view query) rather than the implementation mechanism (creating rules)
- Used in both initial view creation and view replacement scenarios
- The delegation pattern allows for potential future enhancements without breaking the API contract with callers