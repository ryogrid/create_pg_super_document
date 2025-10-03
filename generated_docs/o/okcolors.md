# okcolors

## Location
[src/backend/regex/regc_color.c:916-983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_color.c#L916-L983)

## Overview
The  function promotes subcolors to full colors during regular expression compilation, consolidating the color mapping structure by resolving parent-subcolor relationships.

## Definition

```c
static void
okcolors(struct nfa *nfa,
		 struct colormap *cm)
```
## Detailed Description
This function performs the final phase of color processing in regular expression compilation by promoting subcolors to full colors. It iterates through all colors in the colormap and handles three main scenarios: unused colors, colors that are already subcolors, and colors that have subcolors but are now empty of characters.

When a parent color becomes empty (has no characters), the function transfers all its arcs to the subcolor and frees the parent. When a parent color still contains characters, it creates parallel arcs for the subcolor alongside the existing parent arcs. This process ensures that the NFA correctly represents all character-to-color mappings while optimizing the color structure.

The function is critical for finalizing the color mapping optimization that allows efficient regular expression matching by reducing the number of distinct colors that need to be tracked.

## Parameters / Member Variables
- `*nfa`: Pointer to the NFA structure where arcs will be created or modified
- `*cm`: Pointer to the colormap structure containing color descriptors and relationships
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

## Simplified Source

```c
static void okcolors(struct nfa *nfa, struct colormap *cm)
{
    struct colordesc *cd;
    struct colordesc *end = CDEND(cm);
    struct colordesc *scd;
    struct arc *a;
    color co;
    color sco;

    // Process each color in the colormap
    for (cd = cm->cd, co = 0; cd < end; cd++, co++) {
        sco = cd->sub;

        if (UNUSEDCOLOR(cd) || sco == NOSUB) {
            // Color has no subcolor, no action needed
        }
        else if (sco == co) {
            // This IS a subcolor, let parent handle it
        }
        else if (cd->nschrs == 0 && cd->nuchrs == 0) {
            // Parent is empty, transfer all arcs to subcolor and free parent
            cd->sub = NOSUB;
            scd = &cm->cd[sco];
            assert(scd->nschrs > 0 || scd->nuchrs > 0);
            assert(scd->sub == sco);
            scd->sub = NOSUB;

            // Transfer all arcs from parent to subcolor
            while ((a = cd->arcs) != NULL) {
                assert(a->co == co);
                uncolorchain(cm, a);
                a->co = sco;
                colorchain(cm, a);
            }
            freecolor(cm, co);
        }
        else {
            // Parent has characters, create parallel subcolor arcs
            cd->sub = NOSUB;
            scd = &cm->cd[sco];
            assert(scd->nschrs > 0 || scd->nuchrs > 0);
            assert(scd->sub == sco);
            scd->sub = NOSUB;

            // Create parallel arcs for subcolor
            for (a = cd->arcs; a != NULL; a = a->colorchain) {
                assert(a->co == co);
                newarc(nfa, a->type, sco, a->from, a->to);
            }
        }
    }
}
```