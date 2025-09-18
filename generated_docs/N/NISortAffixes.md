# NISortAffixes

## Location
src/backend/tsearch/spell.c: 1976 - 2027

## Overview
NISortAffixes builds the complete affix tree structures (Prefix and Suffix trees) and manages compound affixes from the imported affix rules.

## Definition


## Detailed Description
This function performs comprehensive processing of imported affix rules to create efficient data structures for spell checking operations. It handles three main tasks:

1. **Compound Affix Processing**: 
   - Sorts affixes using cmpaffix comparator
   - Filters compound affixes (FF_COMPOUNDFLAG) that are actually used in dictionary
   - Creates deduplicated CompoundAffix array for compound word processing
   - Stores only unique minimal compound patterns

2. **Affix Tree Construction**:
   - Determines the boundary between prefix and suffix affixes (firstsuffix)
   - Builds separate prefix and suffix trees using mkANode
   - Creates prefix tree from affixes [0, firstsuffix)
   - Creates suffix tree from affixes [firstsuffix, naffixes)

3. **Void Affix Handling**:
   - Calls mkVoidAffix for both prefix and suffix trees to handle empty replacement patterns
   - Creates special root nodes for deletion-only operations

The resulting tree structures enable fast affix matching during word transformation and spell checking operations.

## Parameters / Member Variables
- : Pointer to IspellDict containing affix configuration and data to be processed

## Dependencies
- Functions called/Symbols referenced:
  - qsort
  - [cmpaffix](../c/cmpaffix.md)
  - [isAffixInUse](../i/isAffixInUse.md)
  - [mkANode](../m/mkANode.md)
  - [mkVoidAffix](../m/mkVoidAffix.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - [strbncmp](../s/strbncmp.md)
  - AFFIX
  - CMPDAffix
  - FF_PREFIX
  - FF_SUFFIX
  - FF_COMPOUNDFLAG
- Called from (representative examples):
  - [dispell_init](../d/dispell_init.md)

## Notes and Other Information
- Returns early if no affixes are present (naffixes == 0)
- Only processes compound affixes that have non-empty replacement strings (replen > 0)
- Sorts affixes before processing to enable efficient tree construction
- CompoundAffix array is reallocated to minimize memory usage after filtering
- The firstsuffix index separates prefix rules from suffix rules in the sorted array
- Void affixes are handled separately to support deletion-only transformations
- This function must be called after affix import but before dictionary can be used for spell checking
- The resulting Prefix and Suffix trees are the core data structures for affix matching operations