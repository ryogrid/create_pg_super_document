# inet_masklen_inclusion_cmp

## Location
[src/backend/utils/adt/network_selfuncs.c:905-938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L905-L938)

## Overview
A specialized comparison function that compares the mask lengths (network prefix lengths) of two inet values according to subnet inclusion operator semantics.

## Definition
static int inet_masklen_inclusion_cmp(inet *left, inet *right, int opr_codenum)

## Detailed Description
This function implements the second stage of inet inclusion comparison, focusing specifically on comparing the network mask lengths of two inet values. It evaluates whether the mask length relationship between the operands satisfies the specified inclusion operator.

The function uses the operator code numbering system from inet_opr_codenum() to determine the acceptance criteria:
- For supernet operators (negative codes): accepts when left has shorter mask (order < 0)
- For overlap operator (code 0): accepts all mask length combinations  
- For subnet operators (positive codes): accepts when left has longer mask (order > 0)
- For equality variants (codes -1, 0, 1): also accepts equal mask lengths (order == 0)

When the mask length relationship doesn't satisfy the operator, it returns the operator code itself as a directional indicator for sorting purposes.

## Parameters / Member Variables
- : Pointer to the left inet operand for mask length comparison
- : Pointer to the right inet operand for mask length comparison
- : Numeric code representing the inclusion operator (from inet_opr_codenum)

## Dependencies
- Functions called/Symbols referenced:
  - ip_bits (extracts mask length from inet value)
- Called from (representative examples):
  - [inet_inclusion_cmp](inet_inclusion_cmp.md) (primary comparison function for inclusion operators)
  - [inet_hist_match_divider](inet_hist_match_divider.md) (histogram partitioning for selectivity estimation)

## Notes and Other Information
The function implements a truth table based on the sign relationships:
- order > 0 && opr_codenum >= 0: Left has longer mask, operator allows subnet relationship  
- order == 0 && opr_codenum >= -1 && opr_codenum <= 1: Equal masks, operator allows equality
- order < 0 && opr_codenum <= 0: Left has shorter mask, operator allows supernet relationship

This design allows the overlap operator (code 0) to accept any mask length combination, while the strict inclusion operators (codes ±2) reject equality cases. The function is a critical component in the two-stage comparison process used for inet inclusion operations.