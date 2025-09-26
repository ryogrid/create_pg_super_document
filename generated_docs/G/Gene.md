# Gene

## Location
[src/include/optimizer/geqo_gene.h:30-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/geqo_gene.h#L30-L31)

## Overview
A typedef representing a single gene in PostgreSQL's Genetic Query Optimizer (GEQO), which is an integer value used to represent a relation identifier in genetic algorithm chromosomes.

## Definition

```c
typedef int Gene;
```
## Detailed Description
Gene is a fundamental data type in PostgreSQL's Genetic Query Optimizer (GEQO) that represents a single gene within a chromosome. Each gene corresponds to a relation (table) identifier in the query optimization context. The GEQO module uses genetic algorithms to solve complex join ordering problems, and genes are the atomic units that make up the chromosomes representing different join orders.

The comment in the source code indicates that using  instead of  was a deliberate design choice for the GEQO implementation, and this should not be changed. This typedef provides abstraction and clarity in the genetic algorithm code, making it clear when variables represent genetic algorithm genes versus regular integers.

## Parameters / Member Variables
This is a simple typedef with no members or parameters - it aliases the  type.

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a basic typedef of int)
- Called from (representative examples):
  - [cx](../c/cx.md) (cycle crossover operation)
  - [gimme_edge_table](../g/gimme_edge_table.md) (edge recombination functions)
  - [gimme_tour](../g/gimme_tour.md)
  - [remove_gene](../r/remove_gene.md)
  - [gimme_gene](../g/gimme_gene.md)
  - [edge_failure](../e/edge_failure.md)
  - [geqo_eval](../g/geqo_eval.md)
  - [gimme_tree](../g/gimme_tree.md)
  - [geqo](../g/geqo.md) (main genetic algorithm function)
  - [geqo_mutation](../g/geqo_mutation.md)
  - [ox1](../o/ox1.md), ox2 (order crossover operations)
  - [pmx](../p/pmx.md) (partially mapped crossover)
  - [alloc_pool](../a/alloc_pool.md)
  - [alloc_chromo](../a/alloc_chromo.md)
  - [px](../p/px.md) (position crossover)
  - [init_tour](../i/init_tour.md)

## Notes and Other Information
- The Gene typedef is explicitly designed to use  rather than  for compatibility and performance reasons in the genetic algorithm implementation
- The source comment warns against changing this typedef, indicating it's a critical design decision
- Genes are typically arranged in arrays (strings) within Chromosome structures to represent complete join orders
- The genetic algorithm operations (crossover, mutation, selection) work on sequences of these Gene values
- Each Gene value represents a relation ID that corresponds to a table in the query being optimized
- The GEQO module uses various genetic operators (PMX, OX1, OX2, ERX, etc.) that manipulate arrays of Gene values
- This abstraction allows the genetic algorithm code to be more readable and maintainable by clearly distinguishing genetic algorithm genes from other integer values in the codebase