# getladfa

## Location
[src/backend/regex/regexec.c:400-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L400-L418)

## Overview
Creates or re-fetches a DFA (Deterministic Finite Automaton) for a LACON subre node during regex execution.

## Definition
```c
static struct dfa *
getladfa(struct vars *v, int n)
```

## Detailed Description
The `getladfa` function manages DFA creation and caching specifically for LACON (lookaround constraint) subre nodes in the regex execution engine. Similar to `getsubdfa`, it implements lazy initialization where DFAs are created only when first needed and cached for reuse. LACONs are special regex constructs for lookahead and lookbehind assertions. The function validates the LACON index and ensures it falls within valid bounds before accessing the lacons array.

## Parameters / Member Variables
- `v`: Pointer to vars structure containing execution state and cached DFAs
- `n`: Index of the LACON in the lacons array (must be > 0 and < nlacons)

## Dependencies
- Functions called/Symbols referenced:
  - newdfa
  - subre
  - cnfa
  - DOMALLOC
- Called from (representative examples):
  - lacon

## Notes and Other Information
- DFAs are cached in `v->ladfas[n]` to avoid recreation during the same regex execution
- Includes assertion to validate LACON index bounds (n > 0 && n < v->g->nlacons)
- LACONs cannot contain backrefs, so no special backref setup is needed
- Returns the cached or newly created DFA for the specified LACON
- The function is static and only used within the regex execution engine