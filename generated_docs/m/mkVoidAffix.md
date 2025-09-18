# mkVoidAffix

## Location
src/backend/tsearch/spell.c: 1907 - 1960

## Overview
mkVoidAffix creates root void nodes in the prefix tree to handle affixes with empty replacement strings, which represent deletion-only operations.

## Definition


## Detailed Description
This function creates special "void" nodes at the root of the affix prefix trees to handle affixes that have empty replacement strings (replen == 0). These affixes represent operations where characters are removed without replacement during word transformation.

The function:
1. **Creates Void Node**: Allocates an AffixNode with the  flag set to 1
2. **Links to Tree**: Connects the new void node to the existing Prefix or Suffix tree root
3. **Collects Empty Affixes**: Identifies and groups all affixes with zero-length replacement strings
4. **Populates Node**: Stores references to all empty-replacement affixes in the void node

This design allows the spell checker to efficiently handle deletion patterns alongside the regular replacement patterns handled by the main prefix tree.

## Parameters / Member Variables
- : Pointer to IspellDict containing the affix configuration and data
- : Boolean flag indicating whether to process suffixes (true) or prefixes (false) 
- : Index marking the boundary between prefixes and suffixes in the Affix array

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - cpalloc
  - ANHRDSZ (AffixNode header size constant)
  - AffixNode
  - [AffixNodeData](../A/AffixNodeData.md)
  - AFFIX
- Called from (representative examples):
  - [NISortAffixes](../N/NISortAffixes.md)

## Notes and Other Information
- Only processes affixes where replen (replacement length) equals 0
- The void node acts as a special root node that chains to the regular prefix tree
- Void nodes are marked with isvoid=1 flag to distinguish them during traversal
- If no empty-replacement affixes exist, the function returns early after creating the basic structure
- The void node structure allows handling of deletion-only transformations efficiently
- Uses separate processing ranges for prefixes [0, startsuffix) and suffixes [startsuffix, naffixes)