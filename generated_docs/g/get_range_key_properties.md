# get_range_key_properties

## Location
[src/backend/partitioning/partbounds.c:4632-4675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L4632-L4675)

## Overview
Extracts and constructs partition key expressions and bound values for a specific column in range partition constraint generation.

## Definition
static void get_range_key_properties(PartitionKey key, int keynum, PartitionRangeDatum *ldatum, PartitionRangeDatum *udatum, ListCell **partexprs_item, Expr **keyCol, Const **lower_val, Const **upper_val)

## Detailed Description
This function serves as a specialized utility for get_qual_for_range, extracting the necessary components for building range partition constraints for a specific column. It constructs an expression representing the partition key column and creates Const nodes for the lower and upper range bounds.

For attribute-based partition keys, it creates Var nodes using makeVar. For expression-based partition keys, it copies the expression from the partition key's expression list. For MINVALUE and MAXVALUE bounds, it returns NULL instead of Const nodes, allowing the caller to handle these special boundary cases appropriately.

The function manages the partexprs_item iterator to advance through expression-based partition keys correctly.

## Parameters / Member Variables
- key: The partition key structure containing column information and expressions
- keynum: Zero-based index of the column being processed
- ldatum: Lower bound datum for this column
- udatum: Upper bound datum for this column  
- partexprs_item: Pointer to iterator through key->partexprs list (may be advanced)
- keyCol: Output parameter for the constructed key column expression
- lower_val: Output parameter for lower bound Const (NULL for MINVALUE)
- upper_val: Output parameter for upper bound Const (NULL for MAXVALUE)

## Dependencies
- Functions called/Symbols referenced:
  - makeVar
  - copyObject
  - [lnext](../l/lnext.md)
  - castNode
- Called from (representative examples):
  - [get_qual_for_range](get_qual_for_range.md)
  - compare_range_bounds

## Notes and Other Information
- Specialized API designed specifically for get_qual_for_range caller requirements
- Handles both attribute-based and expression-based partition keys
- Returns NULL for MINVALUE/MAXVALUE bounds to allow special handling by caller
- All returned structures are freshly allocated with palloc
- Advances partexprs_item iterator when processing expression-based keys
- Error checking ensures correct number of partition key expressions