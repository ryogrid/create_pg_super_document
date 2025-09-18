# spgInitInnerConsistentIn

## Location
src/backend/access/spgist/spgscan.c: 606 - 628

## Overview
Initializes the input structure for calling an SP-GiST opclass inner_consistent method by populating all required fields from scan state and current search context.

## Definition


## Detailed Description
This function serves as a bundle initializer that prepares the spgInnerConsistentIn structure with all necessary information before calling an opclass-specific inner_consistent method. It consolidates data from the scan state, current search item, and inner tuple into a single structure that the inner_consistent method can use to determine which child nodes should be visited.

The function extracts key information including scan keys, order-by clauses, reconstructed values from the traversal path, tuple metadata, and node labels. It performs validation to ensure the current item is not a leaf (since that would indicate incorrect traversal state).

## Parameters
- : spgInnerConsistentIn * - The structure to initialize with input data for inner_consistent
- : SpGistScanOpaque - The scan state containing keys, contexts, and configuration
- : SpGistSearchItem * - The current search item containing traversal state and reconstructed values
- : SpGistInnerTuple - The inner tuple being processed, containing node structure and prefix

## Dependencies
- Functions called/Symbols referenced:
  - SGITDATUM - Extracts the prefix datum from the inner tuple
  - [spgExtractNodeLabels](spgExtractNodeLabels.md) - Extracts node labels from the inner tuple structure
- Called from:
  - [spgInnerTest](spgInnerTest.md) - Uses this to initialize input before calling inner_consistent methods

## Notes and Other Information
- The function includes an assertion to verify that the current item is not a leaf, preventing traversal errors
- The hasPrefix field is derived by checking if prefixSize > 0, providing a boolean convenience flag
- The traversalMemoryContext is passed to allow the inner_consistent method to allocate persistent data
- The nodeLabels extraction handles the complex inner tuple structure to provide easy access to child node information
- All scan keys and order-by data are passed through to allow the opclass method full access to query constraints
- The returnData flag indicates whether the caller needs reconstructed tuple values for result construction