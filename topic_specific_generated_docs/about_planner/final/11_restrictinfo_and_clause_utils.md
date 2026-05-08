# 11. RestrictInfo and Clause Utilities

Prerequisites: [04 PlannerInfo and lifecycle](04_lifecycle_and_entry_points.md), [05 Initial setup and jointree deconstruction](05_initial_setup_and_jointree.md), [07 Equivalence classes and pathkeys](07_equivalence_classes_and_pathkeys.md).

A WHERE clause, a JOIN ON clause, or a HAVING clause is never carried through the planner as a bare `Expr`. It is wrapped in a `RestrictInfo` that caches the relid sets the clause references, the join-method-eligibility flags, the EquivalenceClass pointers, and the selectivity estimates so that `add_paths_to_joinrel`, `set_baserel_size_estimates`, and `cost_*` can interrogate any clause in O(1).

This module documents the `RestrictInfo` data structure and the surrounding clause-utility toolbox: `eval_const_expressions`, the volatility classifiers, the strictness/non-null analyzers, the predicate-implication and predicate-refutation engines, and the OR-clause helpers used by bitmap-OR planning.

Sources:
- `src/backend/optimizer/util/restrictinfo.c` — RestrictInfo construction and clone bookkeeping.
- `src/backend/optimizer/util/clauses.c` — expression-tree introspection (volatility, mutability, leakproofness), constant folding, and AND/OR rewriting.
- `src/backend/optimizer/util/predtest.c` — predicate implication and refutation.
- `src/backend/optimizer/util/orclauses.c` — OR-clause utilities for bitmap OR.
- Struct definition `RestrictInfo` at `src/include/nodes/pathnodes.h:2559`.

## 11.1 Why RestrictInfo exists

When the planner first sees a clause it must answer many questions about it, repeatedly:

- Which base relations does this clause reference (so we know which joinrels can apply it)?
- Is it mergejoinable, hashjoinable, or only a generic filter?
- Does it derive from an EquivalenceClass, and which one?
- Has it been pushed down past an outer join (so the qual must be applied above the OJ)?
- Is it a clone created by outer-join identity 3 (see [Module 20](20_deep_dives.md#202-outer-join-identity-3-and-the-pbcpbc-clone-clause-mechanism))?

Computing those answers from a bare expression every time would be quadratic in the planner's hot loops. `RestrictInfo` is a lazy memo: it caches the answer the first time a question is asked, then every subsequent caller is O(1). Crucially, the same `RestrictInfo` object lives on both `rel->baserestrictinfo` (when the clause filters a base rel) and on `rel->joininfo` (when the clause joins multiple rels), so updates to flags such as `parent_ec` propagate to every site simultaneously.

## 11.2 Symbol table

| Symbol                                  | File:line                                      | Importance | Tier |
|-----------------------------------------|------------------------------------------------|------------|------|
| `RestrictInfo`                          | `src/include/nodes/pathnodes.h:2559`           | 0.82 | 1 |
| `make_restrictinfo`                     | `src/backend/optimizer/util/restrictinfo.c:63` | 0.78 | 1 |
| `restriction_is_or_clause`              | `src/backend/optimizer/util/restrictinfo.c`    | 0.40 | 3 |
| `restriction_is_securely_promotable`    | `src/backend/optimizer/util/restrictinfo.c`    | 0.35 | 3 |
| `eval_const_expressions`                | `src/backend/optimizer/util/clauses.c`         | 0.65 | 2 |
| `contain_volatile_functions`            | `src/backend/optimizer/util/clauses.c`         | 0.55 | 2 |
| `contain_mutable_functions`             | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `contain_subplans`                      | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `find_nonnullable_rels`                 | `src/backend/optimizer/util/clauses.c`         | 0.55 | 2 |
| `find_nonnullable_vars`                 | `src/backend/optimizer/util/clauses.c`         | 0.45 | 3 |
| `find_forced_null_vars`                 | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `predicate_implied_by`                  | `src/backend/optimizer/util/predtest.c`        | 0.55 | 2 |
| `predicate_refuted_by`                  | `src/backend/optimizer/util/predtest.c`        | 0.50 | 2 |
| `expression_returns_set`                | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `expression_returns_set_rows`           | `src/backend/optimizer/util/clauses.c`         | 0.40 | 3 |
| `extract_or_clause`                     | `src/backend/optimizer/util/orclauses.c`       | 0.45 | 3 |
| `is_safe_restriction_clause_for`        | `src/backend/optimizer/util/orclauses.c`       | 0.40 | 3 |
| `make_ands_implicit` / `make_ands_explicit` | `src/backend/optimizer/util/clauses.c`     | 0.45 | 3 |

## 11.3 The RestrictInfo struct

`src/include/nodes/pathnodes.h:2559`:

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

Field-group meaning:
- **Identity** (`clause`, `clause_relids`, `required_relids`, `outer_relids`, `incompatible_relids`): describes *what* the clause says and *where* it can be evaluated.
- **Status flags** (`is_pushed_down`, `can_join`, `pseudoconstant`, `has_clone`, `is_clone`): tell the rest of the planner how to treat the clause.
- **Volatility** (`leakproof`, `has_volatile`, `security_level`): control where the clause may legally appear.
- **Selectivity** (`norm_selec`, `outer_selec`, `eval_cost`): cached per-clause cost-model inputs.
- **Mergejoin** (`mergeopfamilies`, `left_ec`, `right_ec`, `left_em`, `right_em`, `scansel_cache`, `outer_is_left`): determine whether and how the clause can drive a MergeJoin.
- **Hashjoin** (`hashjoinoperator`, `left_bucketsize`, `right_bucketsize`, `left_mcvfreq`, `right_mcvfreq`, `left_hasheqoperator`, `right_hasheqoperator`): determine whether and how the clause can drive a HashJoin.

### 11.3.1 The `RINFO_IS_PUSHED_DOWN` macro

```c
#define RINFO_IS_PUSHED_DOWN(rinfo, joinrelids) \
    ((rinfo)->is_pushed_down || \
     !bms_is_subset((rinfo)->required_relids, joinrelids))
```

The naive `is_pushed_down` flag is no longer sufficient because parameterized paths can push outer-join clauses *below* their syntactic join level. Always use this macro to test "is this clause a filter (post-OJ) at this level?". A "pushed down" RestrictInfo is one that must be evaluated above the outer join that produced it, even if it now appears in a parameterized scan below.

### 11.3.2 `rinfo_serial` and clones

Every RestrictInfo gets a unique-ish `rinfo_serial` so `add_path` can detect "the same logical clause" across paths that differ only in parameterization. The serial is a fresh value from `root->last_rinfo_serial++` except in four cases:

1. Clones produced by outer-join identity 3 share their parent's serial — they are one logical condition expressed in two forms (see [Module 20](20_deep_dives.md#202-outer-join-identity-3-and-the-pbcpbc-clone-clause-mechanism)).
2. Commuted operator versions for index conditions inherit the original serial.
3. A constant-FALSE reduction copies the original's serial so dominated-path detection still works.
4. Child RestrictInfos produced by `adjust_appendrel_attrs` copy their parent's serial.

## 11.4 `make_restrictinfo`

`src/backend/optimizer/util/restrictinfo.c:63`:

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

In one pass over the expression tree this constructor does:

1. Allocates a fresh RestrictInfo (or sets clone fields when `is_clone`).
2. Computes `clause_relids` via `pull_varnos(clause)`. The walker collects `varno + varnullingrels` for each Var and `phrels + phnullingrels` for each PlaceHolderVar.
3. Computes `required_relids` as the union of `clause_relids` and any extra rels passed by the caller (e.g. ojscope rels from `distribute_qual_to_rels`).
4. Detects mergejoinability: if the clause is an `OpExpr` whose operator belongs to a btree-equality opfamily, the function fills `mergeopfamilies` with that list of opfamilies.
5. Detects hashjoinability: if the operator is in a hash opfamily, `hashjoinoperator` is filled, and `left_hasheqoperator` / `right_hasheqoperator` are cached for memoize-style equality compare on the parameterized side.
6. Sets `can_join = (bms_membership(clause_relids) == BMS_MULTIPLE)` — i.e. the clause references at least two base rels.
7. Computes `left_relids` / `right_relids` for any binary OpExpr clause.
8. For OR clauses (`is_orclause`), builds a parallel `orclause` expression where each top-level OR arm is itself a RestrictInfo. This gives `clauselist_selectivity` access to per-arm selectivities.
9. Sets `has_volatile = contain_volatile_functions(clause)` and `leakproof = !contain_leaked_vars(clause)`. These fields are critical for security-barrier views.
10. Assigns `rinfo_serial` unless cloning.

After EquivalenceClass merging is done, `add_eq_member` sets `parent_ec` on the synthetic RestrictInfos generated from EquivalenceClasses. For natural mergejoinable clauses, `process_equivalence` populates `left_ec` / `right_ec` / `left_em` / `right_em` so mergeclause selection can quickly check EC equality.

## 11.5 `eval_const_expressions` — the constant-folding workhorse

```c
Node *eval_const_expressions(PlannerInfo *root, Node *node);
```

`src/backend/optimizer/util/clauses.c`. Called from `preprocess_expression` on every expression in the parse tree (targetlist entries, qual clauses, sortClause expressions, JOIN ON predicates, and so on). It performs:

- **OpExpr / FuncExpr folding**: if all inputs are `Const` and the function is immutable, evaluate it now.
- **Simple SQL function inlining**: `inline_function` substitutes the body of single-statement, SRF-free SQL functions at the call site, exposing the body to further optimization.
- **AND/OR flattening**: nested `BoolExpr(AND, [BoolExpr(AND, [...]), ...])` becomes one flat AND.
- **CASE reduction**: when WHEN conditions are constant, dead branches are eliminated.
- **Named-argument materialization**: converts named-arg calls to positional and supplies default arguments. This is why running `eval_const_expressions` is mandatory for every expression — without it, named-arg calls confuse later code.
- **BoolExpr folding**: AND/OR with a constant true/false subterm collapses.
- **CoalesceExpr reduction**: if the first argument is a non-NULL Const, the whole expression reduces to that Const.

The walker is `eval_const_expressions_mutator`, recursively descending the expression tree. **Volatile functions are never folded**.

## 11.6 Volatility, mutability, and subplan helpers

### 11.6.1 `contain_volatile_functions(node)`

True when the expression contains any function with `pg_proc.provolatile = 'v'`. Used by:

- The HAVING-to-WHERE move check in `preprocess_groupclause`.
- Subquery push-down safety in `pull_up_subqueries`.
- Constant-folding decisions in `eval_const_expressions`.
- Outer-join reduction (a volatile WHERE qual cannot be used to prove non-nullability because each evaluation could return a different result).

### 11.6.2 `contain_mutable_functions(node)`

True when any function is volatile or stable (`'v'` or `'s'`). Used by the index-predicate machinery, the partial-index applicability test, and partition pruning (mutable functions cannot be pruned at plan time because their value may change between planning and execution).

### 11.6.3 `contain_subplans(node)`

True if the expression contains `SubPlan` or `AlternativeSubPlan`. Important because subplans can have side effects (PARAM_EXEC writes) and are not trivially movable.

### 11.6.4 Caching on RestrictInfo

A RestrictInfo's `has_volatile` field is `VOLATILITY_UNKNOWN` until a caller tests it; then the result is cached so repeated tests are O(1). The same lazy memoization applies to `leakproof` via `eval_cost`.

## 11.7 Strictness analysis

### 11.7.1 `find_nonnullable_rels(clause)`

```c
Relids find_nonnullable_rels(Node *clause);
```

Returns the set of relids that *must* be non-NULL for the clause to return TRUE. Strict operators propagate non-nullness: `a + b = 0` proves both `a` and `b` are non-NULL because `+` is strict in both arguments.

The most important caller is `reduce_outer_joins`. An upper-level WHERE clause whose `find_nonnullable_rels` includes the LEFT JOIN's nullable side proves the LEFT JOIN's null-extended rows would be discarded; thus the LEFT JOIN can be demoted to INNER, which unlocks more reorderings in the join search.

### 11.7.2 `find_nonnullable_vars` and `find_forced_null_vars`

Var-level versions of the same idea. `find_forced_null_vars` returns Vars provably equal to NULL in the clause (e.g. `a IS NULL`). `reconsider_outer_join_clauses` uses both to recover EquivalenceClass equality after outer-join reduction.

## 11.8 Predicate implication and refutation

### 11.8.1 `predicate_implied_by(predicate_list, restrictinfo_list, weak)`

`src/backend/optimizer/util/predtest.c`. Returns true when `predicate_list` is logically implied by `restrictinfo_list`. Three production callers:

- **Partial index applicability**: does the index's predicate match the rel's restriction clauses? If yes, set `index->predOK = true`.
- **Constraint exclusion**: a CHECK constraint on a partition rules the partition out if the constraint is refuted by the query.
- **Plan-time partition pruning**: a partition's `partition_qual` must be implied by remaining quals; otherwise the partition cannot be excluded.

### 11.8.2 `predicate_refuted_by(predicate, clauses, weak)`

True when `predicate` is refuted by `clauses` (i.e. the partition definition contradicts a WHERE clause). Used by the same constraint-exclusion pass.

### 11.8.3 The `weak` flag and the proof system

The `weak` flag controls strict-vs-permissive treatment of NULL-valued comparisons. Use `weak = false` for "predicate ⇒ predicate" (partial index) and `weak = true` for "qual cannot be true if the constraint holds" (constraint exclusion).

Recursive descent rules:

- `(A AND B) ⇒ X` if `A ⇒ X` or `B ⇒ X`.
- `(A OR B) ⇒ X` if both `A ⇒ X` and `B ⇒ X`.
- For atomic clauses, per-operator helpers compare structure: `x > 5` implies `x > 0`, `x BETWEEN 1 AND 10` implies `x ≤ 100`, etc.
- `IS NULL` ⇒ `IS NULL`; `IS NOT NULL` follows from any strict operator on the var.

The proof system is intentionally incomplete; the planner accepts false negatives (extra scans) but never false positives (incorrect optimizations).

## 11.9 OR-clause utilities

`src/backend/optimizer/util/orclauses.c`.

### 11.9.1 `extract_or_clause`

Given a RestrictInfo wrapping an OR clause, returns its arms as a list of bare expressions. Useful for building bitmap-OR paths and for selectivity estimation.

### 11.9.2 `extract_restriction_or_clauses` (called from planmain.c)

Walks `joininfo` lists searching for OR clauses where every arm mentions only one rel. Such a clause can be **distributed**: `(a OR b OR c)` where each is a single-rel restriction → emit a fake restriction OR-clause on each rel that is the conjunction of "any arm involving me". This is a weak optimization that occasionally enables bitmap-OR paths that would not otherwise be considered.

### 11.9.3 `make_ands_implicit` and `make_ands_explicit`

Convert between explicit `BoolExpr(AND, ...)` and the implicit-AND form (a List of clauses). Quals are stored in implicit-AND form internally so each conjunct can be distributed independently to the rels it references.

## 11.10 SRF analysis

- `expression_returns_set(node)`: true if the expression contains any set-returning function.
- `expression_returns_set_rows(node)`: estimate the row multiplier introduced by SRFs in the expression. Used by `cost_tlist_sets` to account for ProjectSet costs.

## 11.11 PlaceHolderVar interactions

PlaceHolderVars (PHVs) are a sibling concept to RestrictInfo: they wrap subexpressions that must be evaluated below an outer join but that involve nullable Vars whose values would otherwise be lost. When `make_restrictinfo` runs `pull_varnos(clause)`, PHVs contribute `phrels + phnullingrels` to `clause_relids` exactly the way Vars contribute `varno + varnullingrels`.

The interaction between RestrictInfo and PHV is what lets the planner correctly evaluate complex expressions in the presence of outer joins: a RestrictInfo that contains a PHV can be evaluated only at a level where the PHV's `phrels` are present *and* its `phnullingrels` are above the level. See [Module 20](20_deep_dives.md#203-varnullingrels-and-placeholdervar-above-outer-joins) for a worked example.

## 11.12 Performance characteristics

- `make_restrictinfo`: O(expression size) for `pull_varnos` plus the one-pass classification.
- `eval_const_expressions`: O(expression size); recursive but single-pass.
- `predicate_implied_by` / `predicate_refuted_by`: O(predicate × clauses) with short-circuit evaluation. Partial-index applicability and constraint-exclusion work bound here.
- The cached fields on RestrictInfo amortize repeated questions to O(1).

## 11.13 Cross-references

- EquivalenceClasses and the `parent_ec`/`left_ec`/`right_ec` pointers: [07 Equivalence classes and pathkeys](07_equivalence_classes_and_pathkeys.md).
- The clone mechanism for outer-join identity 3: [05 Initial setup and jointree](05_initial_setup_and_jointree.md) and [Module 20](20_deep_dives.md#202-outer-join-identity-3-and-the-pbcpbc-clone-clause-mechanism).
- Selectivity (where `norm_selec` and `outer_selec` are filled): [10 Cost model and selectivity](10_cost_model_and_selectivity.md).
- Subplans returned by `contain_subplans`: [12 Subqueries and SubLinks](12_subquery_and_sublink.md).
- Path catalog entries that consume RestrictInfos via `joinrestrictinfo`: [18 Path catalog](18_path_catalog.md#join-paths).

Next: [12 Subqueries, SubLinks, and Join Removal](12_subquery_and_sublink.md).
