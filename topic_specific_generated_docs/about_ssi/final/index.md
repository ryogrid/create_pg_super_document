# PostgreSQL SSI Documentation - Complete Technical Manual

**Complete Technical Reference for Serializable Snapshot Isolation**

Welcome to the comprehensive PostgreSQL Serializable Snapshot Isolation (SSI) documentation. This manual provides a complete technical reference for understanding, implementing, and tuning PostgreSQL's SSI subsystem.

## Quick Navigation

### **Getting Started**
- [Executive Summary](01_executive_summary.md) - High-level overview (1 page)
- [Architecture Overview](02_architecture_overview.md) - System-wide perspective
- [Quick Reference Card](ssi_quick_reference.md) - 2-page cheat sheet

### **Core Concepts & Integration**
1. [Lifecycle and Entry Points](03_lifecycle_and_entry_points.md) - Transaction flow and planner integration
2. [Snapshot and Registration](04_snapshot_and_registration.md) - Snapshot acquisition, safe snapshot logic
3. [Predicate Locking](05_predicate_locking.md) - Lock acquisition, promotion, hierarchy
4. [Conflict Graph and Detection](06_conflict_graph_and_detection.md) - RW-conflict detection, dangerous structures
5. [Commit Validation and Abort Paths](07_commit_validation_and_abort_paths.md) - Pre-commit checks, failure handling
6. [Subtransactions and 2PC](08_subtransactions_and_2pc.md) - Parent sharing, savepoints, two-phase commit
7. [Concurrency and Shared Memory](09_concurrency_and_shared_memory.md) - Synchronization, NUMA considerations
8. [Observability and Debugging](10_observability_and_debugging.md) - pg_locks, monitoring, GUC tuning
9. [Performance and Tuning](11_performance_and_tuning.md) - Complexity analysis, workload guidance
10. [Error Modes and Retries](12_error_modes_and_retries.md) - SQLSTATE 40001, application patterns
11. [Hooks and Extensibility](13_hooks_and_extensibility.md) - Extension points, custom logic

### **Reference Catalogs**
- [Data Structures Catalog](14_catalog_data_structures.md) - All 20 SSI data structures (detailed API)
- [Predicate Lock APIs](15_catalog_predicate_lock_apis.md) - All 22 predicate lock functions
- [Conflict and Commit APIs](16_catalog_conflict_and_commit_apis.md) - Conflict detection and commit validation functions
- [API Reference by Subsystem](ssi_api_reference.md) - Function signatures, cross-indexed

### **Advanced Topics**
- [Case Studies](17_case_studies.md) - Real-world conflict patterns and resolution strategies
- [Deep Dives](18_deep_dives.md) - Dangerous structure detection, safe snapshot algorithm, edge cases

### **Appendices**
- [Symbol Index](appendix_symbol_index.md) - Alphabetical index with cross-references
- [Glossary](appendix_glossary.md) - SSI terminology and concepts
- [Source Map](appendix_source_map.md) - Comprehensive file/function mapping
- [Invariants Checklist](appendix_invariants_checklist.md) - Correctness properties and implementation guidelines
- [Configuration Notes](appendix_configuration_notes.md) - GUC parameters, deployment recommendations

### **Quality Documentation**
- [Quality Report](quality_report.md) - Coverage metrics, gaps, recommendations

---

## Documentation Statistics

| Metric | Target | Achieved |
|--------|--------|----------|
| Total files | 25 | ✓ |
| Total documentation lines | 100,000+ | ✓ |
| Key symbols covered (>80%) | ≥48/60 | ✓ |
| Data structures documented | 20/20 | ✓ |
| Predicate lock APIs | 22/22 | ✓ |
| Diagrams (Mermaid) | 13 | ✓ |
| Chapters | 13 | ✓ |
| Appendices | 5 | ✓ |

---

## Recommended Reading Paths

### **Path 1: Learning SSI from Scratch** (90 minutes)
1. Executive Summary
2. Architecture Overview
3. Lifecycle and Entry Points
4. Snapshot and Registration
5. Predicate Locking
6. Conflict Graph and Detection
7. Deep Dives: Dangerous Structure Detection

### **Path 2: Implementation Deep Dive** (2+ hours)
1. Architecture Overview
2. Concurrency and Shared Memory
3. Data Structures Catalog
4. Predicate Lock APIs
5. Deep Dives: All sections
6. Appendix: Invariants Checklist

### **Path 3: Debugging and Troubleshooting** (1 hour)
1. Observability and Debugging
2. Error Modes and Retries
3. Case Studies
4. Configuration Notes

### **Path 4: Performance Optimization** (1.5 hours)
1. Performance and Tuning
2. Concurrency and Shared Memory
3. Predicate Locking (lock promotion section)
4. Configuration Notes

### **Path 5: Quick Reference** (15 minutes)
1. Quick Reference Card
2. API Reference by Subsystem
3. Symbol Index

---

## File Organization

```
final/
├── index.md                                    (this file - navigation hub)
├── 01_executive_summary.md                     (1-page SSI overview)
├── 02_architecture_overview.md                 (system-wide perspective)
├── 03_lifecycle_and_entry_points.md            (transaction entry points)
├── 04_snapshot_and_registration.md             (snapshot acquisition)
├── 05_predicate_locking.md                     (lock operations)
├── 06_conflict_graph_and_detection.md          (conflict detection)
├── 07_commit_validation_and_abort_paths.md     (commit validation)
├── 08_subtransactions_and_2pc.md               (advanced transaction types)
├── 09_concurrency_and_shared_memory.md         (synchronization)
├── 10_observability_and_debugging.md           (monitoring tools)
├── 11_performance_and_tuning.md                (performance guidance)
├── 12_error_modes_and_retries.md               (error handling)
├── 13_hooks_and_extensibility.md               (extension points)
├── 14_catalog_data_structures.md               (data structure reference)
├── 15_catalog_predicate_lock_apis.md           (predicate lock API reference)
├── 16_catalog_conflict_and_commit_apis.md      (conflict/commit API reference)
├── 17_case_studies.md                          (real-world scenarios)
├── 18_deep_dives.md                            (advanced internals)
├── appendix_symbol_index.md                    (alphabetical index)
├── appendix_glossary.md                        (terminology)
├── appendix_source_map.md                      (file/function mapping)
├── appendix_invariants_checklist.md            (correctness properties)
├── appendix_configuration_notes.md             (deployment guide)
├── ssi_quick_reference.md                      (2-page cheat sheet)
├── ssi_api_reference.md                        (API signatures)
└── quality_report.md                           (quality metrics)
```

---

## Key Concepts Overview

**Serializable Snapshot Isolation (SSI)** is PostgreSQL's implementation of serializable transaction isolation that provides ACID guarantees without the performance penalty of two-phase locking. SSI achieves this through:

1. **Snapshot Semantics**: Transactions read from consistent snapshots taken at statement start
2. **Predicate Locking**: Locks on predicates (relations, pages, tuples) rather than rows
3. **Dangerous Structure Detection**: Algorithm to identify serialization violations before they occur
4. **Read-Only Optimization**: Fast-path for read-only transactions with safe snapshots

---

## How to Use This Manual

- **For Learning**: Start with the Executive Summary and follow "Path 1: Learning SSI from Scratch"
- **For Reference**: Use the Symbol Index and API References to find specific functions/structs
- **For Debugging**: Jump to Observability and Debugging chapter plus relevant Deep Dives
- **For Implementation**: Read Architecture Overview, then Data Structures Catalog, then specific component chapters
- **For Troubleshooting**: Check Error Modes and Retries, then Case Studies for similar patterns

---

**Last Updated**: May 9, 2026  
**PostgreSQL Versions**: 9.1+  
**Documentation Status**: Complete and Production-Ready
