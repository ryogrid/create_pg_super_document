# okcolors

## Location
[src/backend/regex/regc_color.c:916-983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L916-L983)

## Overview
The  function promotes subcolors to full colors during regular expression compilation, consolidating the color mapping structure by resolving parent-subcolor relationships.

## Definition


## Detailed Description
This function performs the final phase of color processing in regular expression compilation by promoting subcolors to full colors. It iterates through all colors in the colormap and handles three main scenarios: unused colors, colors that are already subcolors, and colors that have subcolors but are now empty of characters.

When a parent color becomes empty (has no characters), the function transfers all its arcs to the subcolor and frees the parent. When a parent color still contains characters, it creates parallel arcs for the subcolor alongside the existing parent arcs. This process ensures that the NFA correctly represents all character-to-color mappings while optimizing the color structure.

The function is critical for finalizing the color mapping optimization that allows efficient regular expression matching by reducing the number of distinct colors that need to be tracked.

## Parameters / Member Variables
- : Pointer to the NFA structure where arcs will be created or modified
- : Pointer to the colormap structure containing color descriptors and relationships

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get the end of color descriptor array
  - : Macro to check if a color is unused
  - : Removes an arc from a color's chain
  - : Adds an arc to a color's chain
  - : Frees a color that is no longer needed
  - : Creates a new arc in the NFA
  - : Constant indicating no subcolor relationship
- Called from (representative examples):
  - : Error checking wrapper
  - : Arc creation functions
  - : Character class complement processing
  - : Bracket expression processing
  - : Word character processing

## Notes and Other Information
- This is a static helper function used internally within the regex color processing module
- The function contains important logic to avoid creating duplicate arcs when promoting subcolors
- Handles the complex case where both parent and subcolor arcs may need to coexist
- Critical assumption: bracket expression processing doesn't create arcs of both a color and its subcolor between the same endpoints
- Part of the color optimization phase that reduces the complexity of the final NFA