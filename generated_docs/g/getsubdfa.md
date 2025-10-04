# getsubdfa

## Location
[src/backend/regex/regexec.c:372-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L372-L399)

## Overview
Creates or re-fetches a DFA (Deterministic Finite Automaton) for a tree subre node during regex execution.

## Definition
```c
static struct dfa *
getsubdfa(struct vars *v, struct subre *t)
```

## Detailed Description
The `getsubdfa` function manages DFA creation and caching for subre (subexpression) nodes in the regex execution engine. It implements a lazy initialization pattern where DFAs are created only when first needed and then cached for subsequent use. The function handles special setup for backref nodes by configuring additional DFA properties including backref number and min/max quantifiers. The DFA lifecycle is managed by the cleanup step in `pg_regexec()`.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing execution state and cached DFAs
- `t`: Pointer to subre node for which the DFA is needed

## Dependencies
- Functions called/Symbols referenced:
  - [newdfa](../n/newdfa.md)
  - [subre](../s/subre.md)
  - [dfa](../d/dfa.md)
  - [cnfa](../c/cnfa.md)
  - DOMALLOC
- Called from (representative examples):
  - [ccondissect](../c/ccondissect.md)
  - [crevcondissect](../c/crevcondissect.md)
  - [caltdissect](../c/caltdissect.md)
  - [citerdissect](../c/citerdissect.md)
  - [creviterdissect](../c/creviterdissect.md)

## Notes and Other Information
- DFAs are cached in `v->subdfas[t->id]` to avoid recreation during the same regex execution
- Special handling for backref nodes (op == 'b') includes setting backno, backmin, and backmax fields
- Returns NULL on allocation failure
- The function is static and only used within regexec.c

## Simplified Source

```c
static struct dfa *getsubdfa(struct vars *v, struct subre *t) {
    // Check if DFA already exists in cache
    struct dfa *d = v->subdfas[t->id];

    if (d == NULL) {
        // Create new DFA from compiled NFA
        d = newdfa(v, &t->cnfa, &v->g->cmap, DOMALLOC);
        if (d == NULL)
            return NULL;

        // Special setup for backref nodes
        if (t->op == 'b') {
            d->backno = t->backno;
            d->backmin = t->min;
            d->backmax = t->max;
        }

        // Cache the DFA for future use
        v->subdfas[t->id] = d;
    }

    return d;
}
```