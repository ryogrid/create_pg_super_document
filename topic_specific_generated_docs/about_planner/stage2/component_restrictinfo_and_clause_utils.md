# Component: RestrictInfo and Clause Utilities

> Stage 2 documentation for **QUAL_AND_CLAUSE_UTIL**.
> Sources:
> - `src/backend/optimizer/util/restrictinfo.c`: RestrictInfo construction.
> - `src/backend/optimizer/util/clauses.c`: expression-tree introspection
>   and rewrite (volatility, mutability, nonnullability, const-folding).
> - `src/backend/optimizer/util/predtest.c`: predicate implication and
>   refutation tests.
> - `src/backend/optimizer/util/orclauses.c`: OR-clause utilities.
> - Struct definition `RestrictInfo` at `src/include/nodes/pathnodes.h:2559`.

## 1. Why this exists

A WHERE clause, JOIN ON clause, or HAVING clause is not stored in the
planner as a bare `Expr`. It's wrapped in a **`RestrictInfo`** that
carries:

- The clause itself (`clause`).
- Cached relids: `clause_relids`, `required_relids`, `outer_relids`,
  `incompatible_relids`, `left_relids`, `right_relids`.
- Cached selectivity, mergejoinability, hashjoinability flags.
- Pointers to the `EquivalenceClass` (and members) the clause
  participates in.
- Bookkeeping for identity-3 clones and security levels.

The clause utilities provide:
- **Side-effect classification** (volatile / mutable / leakproof).
- **Strictness analysis** (`find_nonnullable_rels`).
- **Predicate implication** for partial-index applicability and
  constraint exclusion.
- **AND/OR rewriting** for CNF/DNF-ish operations.

---

## 2. Symbol table

| Symbol                              | File:line                                      | Importance | Tier |
|-------------------------------------|------------------------------------------------|------------|------|
| `RestrictInfo`                      | `src/include/nodes/pathnodes.h:2559`           | 0.82 | 1 |
| `make_restrictinfo`                 | `src/backend/optimizer/util/restrictinfo.c:63` | 0.78 | 1 |
| `restriction_is_or_clause`          | `src/backend/optimizer/util/restrictinfo.c`    | 0.40 | 3 |
| `restriction_is_securely_promotable`| `src/backend/optimizer/util/restrictinfo.c`    | 0.35 | 3 |
| `eval_const_expressions`            | `src/backend/optimizer/util/clauses.c`         | 0.65 | 2 |
| `contain_volatile_functions`        | `src/backend/optimizer/util/clauses.c`         | 0.55 | 2 |
| `contain_mutable_functions`         | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `contain_subplans`                  | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `find_nonnullable_rels`             | `src/backend/optimizer/util/clauses.c`         | 0.55 | 2 |
| `find_nonnullable_vars`             | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `find_forced_null_vars`             | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `predicate_implied_by`              | `src/backend/optimizer/util/predtest.c`        | 0.55 | 2 |
| `predicate_refuted_by`              | `src/backend/optimizer/util/predtest.c`        | 0.50 | 2 |
| `expression_returns_set`            | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `expression_returns_set_rows`       | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `extract_or_clause`                 | `src/backend/optimizer/util/orclauses.c`       | 0.45 | 3 |
| `is_safe_restriction_clause_for`    | `src/backend/optimizer/util/orclauses.c`       | 0.40 | 3 |
| `make_ands_implicit` / `make_ands_explicit` | `src/backend/optimizer/util/clauses.c` | 0.45 | 3 |

---

## 3. RestrictInfo struct

Source: `src/include/nodes/pathnodes.h:2559`. Key fields:

```c
typedef struct RestrictInfo {
    NodeTag       type;
    Expr         *clause;             /* the represented clause */
    bool          is_pushed_down;     /* eval at non-syntactic level? */
    bool          can_join;           /* possibly usable as joinclause? */
    bool          pseudoconstant;
    bool          has_clone;          /* identity-3: has a sibling clone? */
    bool          is_clone;           /* identity-3: is itself a clone? */
    bool          leakproof;
    VolatileFunctionStatus has_volatile;
    Index         security_level;
    int           num_base_rels;
    Relids        clause_relids;       /* varnos+varnullingrels in clause */
    Relids        required_relids;     /* relids required to evaluate */
    Relids        incompatible_relids; /* relids above which we cannot eval */
    Relids        outer_relids;        /* outer-side rels for OJ clauses */
    Relids        left_relids;
    Relids        right_relids;
    Expr         *orclause;            /* RestrictInfo'd OR if clause is OR */
    int           rinfo_serial;
    EquivalenceClass *parent_ec;       /* generating EC, if any */

    QualCost      eval_cost;
    Selectivity   norm_selec;          /* JOIN_INNER selectivity */
    Selectivity   outer_selec;         /* outer-join selectivity */
    List         *mergeopfamilies;
    EquivalenceClass *left_ec;
    EquivalenceClass *right_ec;
    EquivalenceMember *left_em;
    EquivalenceMember *right_em;
    List         *scansel_cache;       /* MergeScanSelCache list */
    bool          outer_is_left;
    Oid           hashjoinoperator;
    Selectivity   left_bucketsize;
    Selectivity   right_bucketsize;
    Selectivity   left_mcvfreq;
    Selectivity   right_mcvfreq;
    Oid           left_hasheqoperator;
    Oid           right_hasheqoperator;
} RestrictInfo;
```

### 3.1 Critical macro: `RINFO_IS_PUSHED_DOWN`
```c
#define RINFO_IS_PUSHED_DOWN(rinfo, joinrelids) \
    ((rinfo)->is_pushed_down || \
     !bms_is_subset((rinfo)->required_relids, joinrelids))
```
The naive `is_pushed_down` flag isn't sufficient anymore because
parameterized paths can push outer-join clauses below their syntactic
join level. Always use this macro to test "is this clause a filter
(post-OJ) at this level?".

### 3.2 `rinfo_serial` and clones
A unique-ish int identifier. Most RestrictInfos get a fresh value
from `root->last_rinfo_serial++`. Exceptions (per the comment in
`pathnodes.h:2618`):
1. Clones from identity-3 outer-join handling share their
   parent's serial (so add_path knows they're one logical condition).
2. Commuted operator versions for index conditions inherit serial.
3. A constant-FALSE reduction copies the original's serial.
4. Child RestrictInfos copy their parent's serial.

---

## 4. `make_restrictinfo`

### 4.1 Signature
```c
RestrictInfo *
make_restrictinfo(PlannerInfo *root,
                  Expr *clause,
                  bool is_pushed_down,
                  bool has_clone,
                  bool is_clone,
                  bool pseudoconstant,
                  Index security_level,
                  Relids required_relids,
                  Relids incompatible_relids,
                  Relids outer_relids);
```
Source: `src/backend/optimizer/util/restrictinfo.c:63`.

### 4.2 What it does
1. **Allocate** a fresh RestrictInfo node (or set up clone fields).
2. **Compute `clause_relids`** via `pull_varnos(clause)`. This walks
   the expression and collects `varno + varnullingrels` for each Var,
   `phrels + phnullingrels` for PHVs.
3. **Compute `required_relids`**: union of `clause_relids` and any
   additional rels passed by the caller (e.g. ojscope rels from
   `distribute_qual_to_rels`).
4. **Detect mergejoinability**: if the clause is an `OpExpr` of a
   btree-equality opfamily over two sides, it's mergejoinable. Set
   `mergeopfamilies` to the list of qualifying opfamilies.
5. **Detect hashjoinability**: if the clause's operator is in a hash
   opfamily, set `hashjoinoperator`. Cache `left_hasheqoperator` and
   `right_hasheqoperator` for memoize-style equality compare on the
   parameterized side.
6. **Set `can_join`**: true iff `clause_relids` mentions ≥ 2 base
   rels.
7. **Compute `left_relids` / `right_relids`** for any binary op
   clause.
8. **OR clauses** (`is_orclause`): build a parallel `orclause`
   expression where each top-level OR arm is itself a
   sub-RestrictInfo. This gives `clauselist_selectivity` access to
   per-arm selectivities.
9. **Volatility / leakproof flags**:
   - `has_volatile = contain_volatile_functions(clause)`.
   - `leakproof = !contain_leaked_vars(clause)` (used for
     security-barrier views).
10. **Assign `rinfo_serial`** unless cloning.

### 4.3 Equivalence pointers
After EC merging is done, `add_eq_member` sets `parent_ec` on the
synthetic RestrictInfos generated from ECs. For natural mergejoinable
clauses, `process_equivalence` populates `left_ec` / `right_ec` /
`left_em` / `right_em` so mergeclause selection can quickly check EC
equality.

---

## 5. `eval_const_expressions`

Source: `src/backend/optimizer/util/clauses.c`.

```c
Node *eval_const_expressions(PlannerInfo *root, Node *node);
```

The constant-folding workhorse. Highlights:
- **Constant-folds OpExpr/FuncExpr**: if all inputs are Const and
  the function is immutable, evaluate it now.
- **Inlines simple SQL functions**: a SQL function with one statement
  that's an SRF-free SELECT can be inlined as if pasted at the call
  site (`inline_function`).
- **Flattens nested AND/OR**.
- **Reduces `CASE`** when the WHEN conditions are constant.
- **Converts named-arg calls to positional**, materializing default
  arguments. This is why `preprocess_expression` runs it on **every**
  expression — without it, named-arg calls confuse later code.
- **Folds `BoolExpr`** containing constant-true/false subterms.
- **Reduces `CoalesceExpr`** when the first argument is non-NULL Const.

The walker is recursive (`eval_const_expressions_mutator`). Volatile
functions are NEVER folded.

---

## 6. Volatility / mutability / subplans helpers

### 6.1 `contain_volatile_functions(node)`
Returns true if any function in `node` is `pg_proc.provolatile = 'v'`.
Used heavily:
- HAVING-to-WHERE move check.
- Subquery push-down safety.
- Constant-folding decision.
- Outer-join reduction (a volatile WHERE qual can't be used to
  prove non-nullability because each evaluation could differ).

### 6.2 `contain_mutable_functions(node)`
True if any function is `'v'` or `'s'` (volatile or stable). Used by
the index-predicate and partial-index machinery, and partition
pruning (mutable funcs can't be pruned at plan time).

### 6.3 `contain_subplans(node)`
True if the expression contains `SubPlan` or `AlternativeSubPlan`.
Important because subplans can have side effects (PARAM_EXEC writes)
and aren't trivially movable.

### 6.4 Caching on RestrictInfo
A RestrictInfo's `has_volatile` field is `VOLATILITY_UNKNOWN` until a
caller tests it; then the result is cached so repeated tests are O(1).

---

## 7. Strictness analysis

### 7.1 `find_nonnullable_rels`
```c
Relids find_nonnullable_rels(Node *clause);
```
Returns the set of relids that MUST be non-NULL for the clause to
return TRUE. Strict operators propagate non-nullness: `a + b = 0`
implies both `a` and `b` are non-NULL.

Used by `reduce_outer_joins`: an upper-level WHERE clause whose
`find_nonnullable_rels` includes the LEFT JOIN's nullable side proves
the LEFT JOIN's null-extended rows would be discarded; thus we can
demote LEFT to INNER.

### 7.2 `find_nonnullable_vars` / `find_forced_null_vars`
Var-level versions. `find_forced_null_vars` finds Vars provably
**equal to NULL** in the clause (e.g. `a IS NULL`). Used by
`reconsider_outer_join_clauses`.

---

## 8. Predicate implication

### 8.1 `predicate_implied_by(predicate_list, restrictinfo_list, weak)`
Source: `src/backend/optimizer/util/predtest.c`.

True iff `predicate_list` is logically implied by
`restrictinfo_list`. Used for:
- **Partial index** applicability: does the predicate match (be
  implied by) the rel's restriction clauses? Sets `index->predOK`.
- **Constraint exclusion**: a CHECK constraint on a partition rules
  out the partition if the constraint is refuted by the query.
- **Plan-time partition pruning**: a partition's `partition_qual`
  must be implied by remaining quals.

### 8.2 `predicate_refuted_by(predicate, clauses, weak)`
True iff `predicate` is refuted by `clauses` (i.e. the partition
contradicts a WHERE clause). Used for constraint exclusion.

The `weak` flag controls strict-vs-permissive treatment of
NULL-valued comparisons. For predicate-implies-predicate (partial
index), use `weak = false`. For "qual cannot be true if the
constraint holds" (constraint exclusion), use `weak = true`.

### 8.3 The proof system
- Recursive descent: `(A AND B) ⇒ X` if `A ⇒ X` or `B ⇒ X`.
- `(A OR B) ⇒ X` if both `A ⇒ X` and `B ⇒ X`.
- For atomic clauses: per-operator helpers compare structure.
  E.g. `x > 5` implies `x > 0`, `x BETWEEN 1 AND 10` implies
  `x ≤ 100`, etc.
- `IS NULL` ⇒ `IS NULL`; `IS NOT NULL` follows from any strict op
  on the var.

---

## 9. OR-clause utilities

Source: `src/backend/optimizer/util/orclauses.c`.

### 9.1 `extract_or_clause`
Given a RestrictInfo wrapping an OR clause, returns its arms as a
list of bare expressions. Useful for building bitmap-OR paths and
for selectivity estimation.

### 9.2 `extract_restriction_or_clauses` (planmain.c calls this)
Walks `joininfo` lists searching for OR clauses where every arm
mentions only one rel. Such a clause can be **distributed**:
`(a OR b OR c)` where each is a single-rel restriction → emit a
fake restriction OR-clause on each rel that's the conjunction of
"any arm involving me". This is a weak optimization that sometimes
enables index-bitmap-OR paths that wouldn't otherwise be considered.

### 9.3 `make_ands_implicit` / `make_ands_explicit`
Convert between explicit `BoolExpr(AND, ...)` and the implicit-AND
form (a List of clauses). Quals are stored in implicit-AND form
internally so each conjunct can be distributed independently.

---

## 10. SRF analysis

- `expression_returns_set(node)` — true if the expression contains
  any set-returning function (SRF).
- `expression_returns_set_rows(node)` — estimate the row multiplier
  introduced by SRFs in the expression. Used by `cost_tlist_sets` to
  account for ProjectSet costs.

---

## 11. Performance characteristics

- `make_restrictinfo`: O(expression size) for `pull_varnos` and
  one-pass classification.
- `eval_const_expressions`: O(expression size); recursive.
- `predicate_implied_by` / `predicate_refuted_by`: roughly O(predicate
  × clauses) but with short-circuiting; partial-index and
  constraint-exclusion work bound here.

---

## 12. Cross-references

- ECs and RestrictInfo `parent_ec`/`left_ec`/`right_ec`:
  `component_equivalence_classes_and_pathkeys.md`
- Clone mechanism for identity 3:
  `component_initial_setup_and_jointree.md`
- Selectivity (where RestrictInfo's `norm_selec` / `outer_selec` are
  filled): `component_cost_model_and_selectivity.md`
- Subplans returned by `contain_subplans`:
  `component_subquery_and_sublink.md`
