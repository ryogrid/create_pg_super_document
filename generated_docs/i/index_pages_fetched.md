# index_pages_fetched

## Location
[src/backend/optimizer/path/costsize.c:898-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L898-L962)

## Overview
Estimates the number of pages actually fetched from storage after accounting for cache effects using the Mackert and Lohman formula.

## Definition
```c
double index_pages_fetched(double tuples_fetched, BlockNumber pages,
                           double index_pages, PlannerInfo *root)
```

## Detailed Description
The `index_pages_fetched` function implements the Mackert and Lohman approximation for estimating actual I/O operations required when scanning a table or index, considering the effects of buffer cache. The algorithm uses a sophisticated mathematical model that accounts for cache hit rates based on the relationship between table size, selectivity, and available cache space.

The function applies different formulas depending on whether the table fits entirely in cache (T <= b) or not. For larger tables, it uses a piecewise function that transitions between high cache effectiveness for small scans and lower effectiveness for large scans. The model pro-rates the effective cache size across all tables and indexes in the query to estimate the portion available for the current operation.

## Parameters / Member Variables
- `tuples_fetched`: Number of tuples to be fetched (product of selectivity and table size)
- `pages`: Total number of pages in the table or index being scanned
- `index_pages`: Additional index pages that compete for cache space  
- `root`: PlannerInfo containing total_table_pages for cache size calculation

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type)
  - [PlannerInfo](../P/PlannerInfo.md) (structure)
  - effective_cache_size (global variable)
  - Max (macro)
  - Assert (macro)
  - ceil (math function)
- Called from (representative examples):
  - [cost_index](../c/cost_index.md)
  - [compute_bitmap_pages](../c/compute_bitmap_pages.md)
  - [genericcostestimate](../g/genericcostestimate.md)
  - [gincostestimate](../g/gincostestimate.md)

## Notes and Other Information
The function implements the seminal Mackert and Lohman I/O model from their 1989 ACM Transactions paper, which remains a foundational algorithm in database query optimization. The model accounts for LRU buffer replacement and provides three different formulas based on the relationship between table size, cache size, and scan selectivity. The pro-rating of cache space across multiple relations reflects the reality of concurrent access patterns in complex queries, though it necessarily makes simplifying assumptions about actual memory competition.

## Simplified Source

```c
double
index_pages_fetched(double tuples_fetched, BlockNumber pages, double index_pages, PlannerInfo *root)
{
    double pages_fetched;
    double total_pages;
    double T, b;

    // T = number of pages in table (ensure >= 1)
    T = (pages > 1) ? (double) pages : 1.0;

    // Calculate total competing pages and pro-rate cache
    total_pages = root->total_table_pages + index_pages;
    total_pages = Max(total_pages, 1.0);
    b = (double) effective_cache_size * T / total_pages;

    // Force cache size to be positive and integral
    if (b <= 1.0)
        b = 1.0;
    else
        b = ceil(b);

    // Apply Mackert and Lohman formula
    if (T <= b) {
        // Table fits in cache
        pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        if (pages_fetched >= T)
            pages_fetched = T;
        else
            pages_fetched = ceil(pages_fetched);
    }
    else {
        // Table larger than cache
        double lim = (2.0 * T * b) / (2.0 * T - b);

        if (tuples_fetched <= lim) {
            pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        }
        else {
            pages_fetched = b + (tuples_fetched - lim) * (T - b) / T;
        }
        pages_fetched = ceil(pages_fetched);
    }

    return pages_fetched;
}
```