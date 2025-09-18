# varstr_levenshtein_less_equal

## Location
src/backend/utils/adt/levenshtein.c: 68 - 95

## Overview
Computes the Levenshtein distance between two strings with custom costs for insertions, deletions, and substitutions, with an optimization to return early when the distance exceeds a specified maximum threshold.

## Definition


## Detailed Description
This function is a specialized variant of the Levenshtein distance algorithm that includes an important optimization: it can terminate early when the computed distance exceeds a given maximum threshold (max_d). This makes it particularly useful for fuzzy matching scenarios where you only care whether strings are "close enough" rather than the exact distance.

The function uses a space-optimized dynamic programming approach, maintaining only two rows of the distance matrix at any time instead of the full (m+1)×(n+1) matrix. For the _less_equal variant, it further optimizes by using sliding start_column and stop_column boundaries to avoid computing cells that cannot possibly contribute to a solution within the max_d bound.

The algorithm handles both single-byte and multi-byte character encodings properly, with a fast-path optimization for single-byte characters. It includes security protections to prevent excessive CPU and memory usage by limiting string lengths (unless the caller is trusted).

## Parameters / Member Variables
- : Source string to transform from (not necessarily null-terminated)
- : Length of source string in bytes
- : Target string to transform to (not necessarily null-terminated)  
- : Length of target string in bytes
- : Cost of inserting a character
- : Cost of deleting a character
- : Cost of substituting a character
- : Maximum distance threshold; if >= 0, function returns max_d + 1 when actual distance exceeds this value
- : If true, caller is responsible for reasonable input sizes; if false, enforces MAX_LEVENSHTEIN_STRLEN limit

## Dependencies
- Functions called/Symbols referenced:
  - LEVENSHTEIN_LESS_EQUAL (preprocessor macro)
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [pg_mblen](../p/pg_mblen.md)
  - [palloc](../p/palloc.md)
  - ereport
  - [rest_of_char_same](../r/rest_of_char_same.md)
  - Min (macro)
- Called from (representative examples):
  - [updateFuzzyAttrMatchState](../u/updateFuzzyAttrMatchState.md) (src/backend/parser/parse_relation.c:608)
  - [searchRangeTableForCol](../s/searchRangeTableForCol.md) (src/backend/parser/parse_relation.c:991)
  - [updateClosestMatch](../u/updateClosestMatch.md) (src/backend/utils/adt/varlena.c:6226)

## Notes and Other Information
This function is created through conditional compilation of levenshtein.c when LEVENSHTEIN_LESS_EQUAL is defined. The same source file generates both varstr_levenshtein() (standard version) and varstr_levenshtein_less_equal() (bounded version) by being included twice in varlena.c with different preprocessor definitions.

The early termination optimization uses theoretical minimum and maximum distance bounds to slide the computation window (start_column/stop_column) during each iteration, significantly improving performance when max_d is much smaller than the theoretical maximum distance.

Security note: When trusted=false, the function enforces a maximum string length of MAX_LEVENSHTEIN_STRLEN (255 characters) to prevent resource exhaustion attacks. Trusted callers are expected to implement their own reasonable limits.

The function returns max_d + 1 (rather than the actual distance) when the distance exceeds the threshold, allowing callers to distinguish between "within threshold" and "exceeds threshold" cases efficiently.