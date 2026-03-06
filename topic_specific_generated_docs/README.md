# PostgreSQL Topic-Specific Documentation

This directory contains in-depth implementation documentation for seven major PostgreSQL subsystems. Each topic is documented through technical prose, architectural diagrams, and API references derived directly from the PostgreSQL source code.

## How to Read This Documentation

### Directory Structure Conventions

Two structural patterns are used across topics:

**Pattern A — Staged with `final/` directory** (`about_buffer_management`, `about_executor`)
The `final/` subdirectory contains the polished, reader-facing documents organized as numbered chapters. The sibling `stage1/` and `stage2/` directories are intermediate work artifacts and are not intended for reading.

**Pattern B — Flat chapter layout** (all other topics)
Markdown files at the topic root are the primary documents, organized as numbered chapters (`01_`, `02_`, …) or as named component files.

### Mermaid Diagrams

Each topic has a `diagrams/` subdirectory containing `.mermaid` files. These diagrams supplement the text but are **not rendered inline** in the markdown files — open them separately in a Mermaid-compatible viewer (e.g., the [Mermaid Live Editor](https://mermaid.live/), VS Code with a Mermaid extension, or GitHub's native `.mermaid` file rendering).

---

## Topics

| Topic | Focus | Entry Point |
|---|---|---|
| [Write-Ahead Log (WAL)](#write-ahead-log-wal) | WAL generation, writing, recovery, replication | [`about_wal/wal_complete_documentation.md`](about_wal/wal_complete_documentation.md) |
| [Buffer Management](#buffer-management) | Shared buffer pool, page I/O, eviction | [`about_buffer_management/final/index.md`](about_buffer_management/final/index.md) |
| [Checkpointing](#checkpointing) | Checkpoint control, buffer flushing, WAL integration | [`about_checkpointing/checkpointing_complete_documentation.md`](about_checkpointing/checkpointing_complete_documentation.md) |
| [MVCC](#mvcc-multi-version-concurrency-control) | Tuple versioning, snapshots, visibility, vacuum | [`about_mvcc/index.md`](about_mvcc/index.md) |
| [Executor](#executor) | Query execution, Volcano model, join/aggregate nodes | [`about_executor/final/index.md`](about_executor/final/index.md) |
| [Streaming Replication — Primary Side](#streaming-replication--primary-side) | WAL generation to walsender, synchronous replication | [`about_primary_side_of_streaming_replication/index.md`](about_primary_side_of_streaming_replication/index.md) |
| [Streaming Replication — Full System](#streaming-replication--full-system) | End-to-end replication, standby, inter-process coordination | [`about_streaming_replication/index.md`](about_streaming_replication/index.md) |

---

## Write-Ahead Log (WAL)

WAL is the foundation of PostgreSQL's durability and replication. This documentation covers WAL record generation, the WAL writer process, replication sender/receiver roles, and crash recovery.

**Entry point**: [wal_complete_documentation.md](about_wal/wal_complete_documentation.md)

### Documents

| File | Description |
|---|---|
| [wal_complete_documentation.md](about_wal/wal_complete_documentation.md) | Comprehensive WAL architecture and implementation |
| [component_wal_generation.md](about_wal/component_wal_generation.md) | WAL record creation and LSN assignment |
| [component_wal_writing.md](about_wal/component_wal_writing.md) | WAL writer process and buffer flushing |
| [component_replication_sender.md](about_wal/component_replication_sender.md) | Walsender process internals |
| [component_replication_receiver.md](about_wal/component_replication_receiver.md) | Walreceiver process internals |
| [component_recovery.md](about_wal/component_recovery.md) | Crash recovery and REDO |
| [wal_api_reference.md](about_wal/wal_api_reference.md) | Key functions and data structures |
| [wal_quick_reference.md](about_wal/wal_quick_reference.md) | Quick lookup guide |

### Diagrams (`about_wal/diagrams/`)

| File | Description |
|---|---|
| [wal_architecture_overview.mermaid](about_wal/diagrams/wal_architecture_overview.mermaid) | High-level WAL subsystem architecture |
| [wal_record_generation_sequence.mermaid](about_wal/diagrams/wal_record_generation_sequence.mermaid) | Sequence: WAL record creation to disk |
| [replication_data_flow.mermaid](about_wal/diagrams/replication_data_flow.mermaid) | Data flow from primary to standby |
| [recovery_process_sequence.mermaid](about_wal/diagrams/recovery_process_sequence.mermaid) | Crash recovery REDO sequence |
| [standby_state_machine.mermaid](about_wal/diagrams/standby_state_machine.mermaid) | Standby process state transitions |

---

## Buffer Management

The shared buffer manager is the primary I/O cache for PostgreSQL. This documentation covers the buffer pool architecture, the clock-sweep eviction algorithm, pin/lock protocols, page layouts, dirty buffer writeback, and integration with the storage manager and WAL.

> **Note**: This topic uses Pattern A. Read documents from the [`final/`](about_buffer_management/final/) directory. The `stage1/` and `stage2/` directories are intermediate artifacts.

**Entry point**: [final/index.md](about_buffer_management/final/index.md)

### Documents (`about_buffer_management/final/`)

| File | Description |
|---|---|
| [01_executive_summary.md](about_buffer_management/final/01_executive_summary.md) | Overview and design philosophy |
| [02_architecture_overview.md](about_buffer_management/final/02_architecture_overview.md) | Subsystem architecture |
| [03_buffer_pool_architecture.md](about_buffer_management/final/03_buffer_pool_architecture.md) | Buffer pool data structures |
| [04_buffer_lookup_and_hashtable.md](about_buffer_management/final/04_buffer_lookup_and_hashtable.md) | Buffer tag hash table |
| [05_buffer_access_protocol.md](about_buffer_management/final/05_buffer_access_protocol.md) | ReadBuffer / ReleaseBuffer protocol |
| [06_page_concurrency_control.md](about_buffer_management/final/06_page_concurrency_control.md) | Pin counts and content locks |
| [07_buffer_replacement_policy.md](about_buffer_management/final/07_buffer_replacement_policy.md) | Clock-sweep and ring buffer strategies |
| [08_page_layout_and_types.md](about_buffer_management/final/08_page_layout_and_types.md) | Page header and item layout |
| [09_dirty_buffer_and_writeback.md](about_buffer_management/final/09_dirty_buffer_and_writeback.md) | Dirty tracking and writeback pipeline |
| [10_wal_integration.md](about_buffer_management/final/10_wal_integration.md) | WAL integration and full-page writes |
| [11_storage_manager.md](about_buffer_management/final/11_storage_manager.md) | Storage manager (smgr) interface |
| [12_data_movement_and_durability.md](about_buffer_management/final/12_data_movement_and_durability.md) | Data movement and durability |
| [13_local_buffers.md](about_buffer_management/final/13_local_buffers.md) | Local (temporary relation) buffers |
| [14_access_method_integration.md](about_buffer_management/final/14_access_method_integration.md) | Integration with table/index access methods |
| [15_deep_dives.md](about_buffer_management/final/15_deep_dives.md) | Deep dives into critical code paths |
| [appendix_data_structures.md](about_buffer_management/final/appendix_data_structures.md) | Data structure reference |
| [appendix_glossary.md](about_buffer_management/final/appendix_glossary.md) | Glossary |
| [appendix_guc_parameters.md](about_buffer_management/final/appendix_guc_parameters.md) | GUC parameters |
| [appendix_symbol_index.md](about_buffer_management/final/appendix_symbol_index.md) | Symbol index |
| [buffer_mgmt_api_reference.md](about_buffer_management/final/buffer_mgmt_api_reference.md) | API reference |
| [buffer_mgmt_quick_reference.md](about_buffer_management/final/buffer_mgmt_quick_reference.md) | Quick reference |

### Diagrams (`about_buffer_management/diagrams/`)

| File | Description |
|---|---|
| [buffer_pool_layout.mermaid](about_buffer_management/diagrams/buffer_pool_layout.mermaid) | Shared buffer pool layout |
| [readbuffer_flow.mermaid](about_buffer_management/diagrams/readbuffer_flow.mermaid) | ReadBuffer control flow |
| [clock_sweep.mermaid](about_buffer_management/diagrams/clock_sweep.mermaid) | Clock-sweep eviction algorithm |
| [pin_lock_protocol.mermaid](about_buffer_management/diagrams/pin_lock_protocol.mermaid) | Pin and content lock protocol |
| [lock_hierarchy.mermaid](about_buffer_management/diagrams/lock_hierarchy.mermaid) | Lock hierarchy |
| [page_layout.mermaid](about_buffer_management/diagrams/page_layout.mermaid) | Page header and item layout |
| [writeback_pipeline.mermaid](about_buffer_management/diagrams/writeback_pipeline.mermaid) | Dirty buffer writeback pipeline |
| [ring_buffer_strategies.mermaid](about_buffer_management/diagrams/ring_buffer_strategies.mermaid) | Ring buffer replacement strategies |
| [storage_stack.mermaid](about_buffer_management/diagrams/storage_stack.mermaid) | Storage manager stack |

---

## Checkpointing

Checkpointing periodically flushes dirty buffers and advances the WAL recovery horizon to bound crash-recovery time. This documentation covers the checkpoint controller, background writer coordination, buffer flushing sequences, full-page writes, and WAL integration.

**Entry point**: [checkpointing_complete_documentation.md](about_checkpointing/checkpointing_complete_documentation.md)

### Documents

| File | Description |
|---|---|
| [checkpointing_complete_documentation.md](about_checkpointing/checkpointing_complete_documentation.md) | Comprehensive checkpointing documentation |
| [component_checkpoint_control.md](about_checkpointing/component_checkpoint_control.md) | Checkpoint controller (checkpointer process) |
| [component_background_writer.md](about_checkpointing/component_background_writer.md) | Background writer (bgwriter) process |
| [component_buffer_flushing.md](about_checkpointing/component_buffer_flushing.md) | Buffer flush sequencing |
| [component_wal_coordination.md](about_checkpointing/component_wal_coordination.md) | WAL coordination during checkpoint |
| [component_recovery_points.md](about_checkpointing/component_recovery_points.md) | Recovery points and restart LSN |
| [checkpointing_api_reference.md](about_checkpointing/checkpointing_api_reference.md) | API reference |
| [checkpointing_quick_reference.md](about_checkpointing/checkpointing_quick_reference.md) | Quick reference |

### Diagrams (`about_checkpointing/diagrams/`)

| File | Description |
|---|---|
| [checkpoint_architecture_overview.mermaid](about_checkpointing/diagrams/checkpoint_architecture_overview.mermaid) | Checkpoint subsystem architecture |
| [checkpoint_triggering_flow.mermaid](about_checkpointing/diagrams/checkpoint_triggering_flow.mermaid) | Checkpoint trigger conditions and flow |
| [checkpoint_states_transitions.mermaid](about_checkpointing/diagrams/checkpoint_states_transitions.mermaid) | Checkpointer state machine |
| [buffer_flushing_sequence.mermaid](about_checkpointing/diagrams/buffer_flushing_sequence.mermaid) | Dirty buffer flush sequence |
| [fpw_checkpoint_logic.mermaid](about_checkpointing/diagrams/fpw_checkpoint_logic.mermaid) | Full-page write logic during checkpoint |
| [wal_checkpoint_timeline.mermaid](about_checkpointing/diagrams/wal_checkpoint_timeline.mermaid) | WAL and checkpoint timeline |

---

## MVCC (Multi-Version Concurrency Control)

MVCC enables concurrent readers and writers without blocking. This documentation covers transaction IDs, tuple versioning (xmin/xmax), visibility rules, snapshot acquisition, CLOG transaction status, and vacuum/freezing.

**Entry point**: [index.md](about_mvcc/index.md)

### Documents

| File | Description |
|---|---|
| [01_executive_summary.md](about_mvcc/01_executive_summary.md) | Overview and design philosophy |
| [02_architecture_overview.md](about_mvcc/02_architecture_overview.md) | MVCC subsystem architecture |
| [03_transaction_lifecycle.md](about_mvcc/03_transaction_lifecycle.md) | Transaction begin, commit, abort |
| [04_tuple_versioning.md](about_mvcc/04_tuple_versioning.md) | xmin/xmax and tuple version chains |
| [05_visibility_rules.md](about_mvcc/05_visibility_rules.md) | Tuple visibility algorithm |
| [06_snapshot_management.md](about_mvcc/06_snapshot_management.md) | Snapshot acquisition and types |
| [07_concurrency_infrastructure.md](about_mvcc/07_concurrency_infrastructure.md) | Locking and concurrency primitives |
| [08_clog_transaction_status.md](about_mvcc/08_clog_transaction_status.md) | CLOG / pg_xact internals |
| [09_vacuum_and_freezing.md](about_mvcc/09_vacuum_and_freezing.md) | Vacuum, autovacuum, and freezing |
| [10_deep_dives.md](about_mvcc/10_deep_dives.md) | Deep dives into critical code paths |
| [appendix_data_structures.md](about_mvcc/appendix_data_structures.md) | Data structure reference |
| [appendix_glossary.md](about_mvcc/appendix_glossary.md) | Glossary |
| [appendix_symbol_index.md](about_mvcc/appendix_symbol_index.md) | Symbol index |
| [mvcc_api_reference.md](about_mvcc/mvcc_api_reference.md) | API reference |
| [mvcc_quick_reference.md](about_mvcc/mvcc_quick_reference.md) | Quick reference |

### Diagrams (`about_mvcc/diagrams/`)

| File | Description |
|---|---|
| [transaction_lifecycle.mermaid](about_mvcc/diagrams/transaction_lifecycle.mermaid) | Transaction state transitions |
| [tuple_version_chain.mermaid](about_mvcc/diagrams/tuple_version_chain.mermaid) | Tuple version chain structure |
| [mvcc_visibility_flowchart.mermaid](about_mvcc/diagrams/mvcc_visibility_flowchart.mermaid) | Tuple visibility decision flowchart |
| [snapshot_acquisition.mermaid](about_mvcc/diagrams/snapshot_acquisition.mermaid) | Snapshot acquisition sequence |
| [clog_status_transitions.mermaid](about_mvcc/diagrams/clog_status_transitions.mermaid) | CLOG transaction status transitions |
| [isolation_level_comparison.mermaid](about_mvcc/diagrams/isolation_level_comparison.mermaid) | Isolation level behavior comparison |
| [shared_memory_layout.mermaid](about_mvcc/diagrams/shared_memory_layout.mermaid) | MVCC shared memory layout |
| [vacuum_cleanup_flow.mermaid](about_mvcc/diagrams/vacuum_cleanup_flow.mermaid) | Vacuum dead tuple cleanup flow |

---

## Executor

The query executor processes plan trees produced by the planner using the Volcano iterator model. This documentation covers the executor lifecycle, the `ExecProcNode`/`ExecInitNode`/`ExecEndNode` protocol, tuple table slots, expression evaluation, join algorithms, aggregation, parallel query, and the `ModifyTable` node.

> **Note**: This topic uses Pattern A. Read documents from the [`final/`](about_executor/final/) directory. The `stage1/` and `stage2/` directories are intermediate artifacts.

**Entry point**: [final/index.md](about_executor/final/index.md)

### Documents (`about_executor/final/`)

| File | Description |
|---|---|
| [01_executive_summary.md](about_executor/final/01_executive_summary.md) | Overview and design philosophy |
| [02_architecture_overview.md](about_executor/final/02_architecture_overview.md) | Executor subsystem architecture |
| [03_executor_lifecycle.md](about_executor/final/03_executor_lifecycle.md) | Init / Exec / End lifecycle |
| [04_volcano_iterator_model.md](about_executor/final/04_volcano_iterator_model.md) | Volcano / iterator model |
| [05_tuple_table_slot.md](about_executor/final/05_tuple_table_slot.md) | TupleTableSlot internals |
| [06_expression_evaluation.md](about_executor/final/06_expression_evaluation.md) | Expression evaluation pipeline |
| [07_memory_context_management.md](about_executor/final/07_memory_context_management.md) | Memory context management |
| [08_scan_infrastructure.md](about_executor/final/08_scan_infrastructure.md) | Scan nodes and table AM interface |
| [09_join_infrastructure.md](about_executor/final/09_join_infrastructure.md) | Join node infrastructure |
| [10_aggregation_and_grouping.md](about_executor/final/10_aggregation_and_grouping.md) | Aggregation and grouping |
| [11_modifytable.md](about_executor/final/11_modifytable.md) | ModifyTable (INSERT/UPDATE/DELETE) |
| [12_parallel_execution.md](about_executor/final/12_parallel_execution.md) | Parallel query execution |
| [13_planner_interface.md](about_executor/final/13_planner_interface.md) | Planner-executor interface |
| [14_spi.md](about_executor/final/14_spi.md) | Server Programming Interface (SPI) |
| [15_node_catalog_scan.md](about_executor/final/15_node_catalog_scan.md) | Scan node catalog |
| [16_node_catalog_join.md](about_executor/final/16_node_catalog_join.md) | Join node catalog |
| [17_node_catalog_sort_aggregate.md](about_executor/final/17_node_catalog_sort_aggregate.md) | Sort and aggregate node catalog |
| [18_node_catalog_modify_control.md](about_executor/final/18_node_catalog_modify_control.md) | Modify and control node catalog |
| [19_node_catalog_parallel.md](about_executor/final/19_node_catalog_parallel.md) | Parallel node catalog |
| [20_deep_dives.md](about_executor/final/20_deep_dives.md) | Deep dives into critical code paths |
| [appendix_data_structures.md](about_executor/final/appendix_data_structures.md) | Data structure reference |
| [appendix_glossary.md](about_executor/final/appendix_glossary.md) | Glossary |
| [appendix_symbol_index.md](about_executor/final/appendix_symbol_index.md) | Symbol index |
| [appendix_node_quick_reference.md](about_executor/final/appendix_node_quick_reference.md) | Node quick reference |
| [executor_api_reference.md](about_executor/final/executor_api_reference.md) | API reference |
| [executor_quick_reference.md](about_executor/final/executor_quick_reference.md) | Quick reference |

### Diagrams (`about_executor/diagrams/`)

| File | Description |
|---|---|
| [executor_lifecycle.mermaid](about_executor/diagrams/executor_lifecycle.mermaid) | Init / Exec / End lifecycle |
| [volcano_tuple_flow.mermaid](about_executor/diagrams/volcano_tuple_flow.mermaid) | Volcano tuple pull flow |
| [node_dispatch_flowchart.mermaid](about_executor/diagrams/node_dispatch_flowchart.mermaid) | ExecProcNode dispatch flowchart |
| [node_type_taxonomy.mermaid](about_executor/diagrams/node_type_taxonomy.mermaid) | Executor node type taxonomy |
| [tuple_slot_hierarchy.mermaid](about_executor/diagrams/tuple_slot_hierarchy.mermaid) | TupleTableSlot type hierarchy |
| [expression_pipeline.mermaid](about_executor/diagrams/expression_pipeline.mermaid) | Expression evaluation pipeline |
| [hashjoin_two_phase.mermaid](about_executor/diagrams/hashjoin_two_phase.mermaid) | Hash join two-phase execution |
| [mergejoin_state_machine.mermaid](about_executor/diagrams/mergejoin_state_machine.mermaid) | Merge join state machine |
| [parallel_query_architecture.mermaid](about_executor/diagrams/parallel_query_architecture.mermaid) | Parallel query architecture |
| [modifytable_dispatch.mermaid](about_executor/diagrams/modifytable_dispatch.mermaid) | ModifyTable operation dispatch |

---

## Streaming Replication — Primary Side

This documentation focuses on the primary server's role in streaming replication: WAL generation and LSN assignment, WAL persistence, walsender transmission, keepalive monitoring, standby response handling, synchronous replication wait/release, and client response.

**Entry point**: [index.md](about_primary_side_of_streaming_replication/index.md)

### Documents

| File | Description |
|---|---|
| [01_architecture_overview.md](about_primary_side_of_streaming_replication/01_architecture_overview.md) | Overall primary-side architecture |
| [02_wal_generation_lsn.md](about_primary_side_of_streaming_replication/02_wal_generation_lsn.md) | WAL generation and LSN assignment |
| [03_wal_persistence.md](about_primary_side_of_streaming_replication/03_wal_persistence.md) | WAL persistence (flush to disk) |
| [04_walsender_transmission.md](about_primary_side_of_streaming_replication/04_walsender_transmission.md) | Walsender data transmission |
| [05_keepalive_monitoring.md](about_primary_side_of_streaming_replication/05_keepalive_monitoring.md) | Keepalive and monitoring |
| [06_standby_response.md](about_primary_side_of_streaming_replication/06_standby_response.md) | Standby response message handling |
| [07_sync_wait_release.md](about_primary_side_of_streaming_replication/07_sync_wait_release.md) | Synchronous replication wait/release |
| [08_client_response.md](about_primary_side_of_streaming_replication/08_client_response.md) | Client response after commit |
| [appendix_config_params.md](about_primary_side_of_streaming_replication/appendix_config_params.md) | Configuration parameters |
| [appendix_glossary.md](about_primary_side_of_streaming_replication/appendix_glossary.md) | Glossary |
| [appendix_symbol_index.md](about_primary_side_of_streaming_replication/appendix_symbol_index.md) | Symbol index |

### Diagrams (`about_primary_side_of_streaming_replication/diagrams/`)

Diagram file numbers correspond to the related chapter numbers.

| File | Description |
|---|---|
| [01_overall_architecture.mermaid](about_primary_side_of_streaming_replication/diagrams/01_overall_architecture.mermaid) | Primary-side overall architecture |
| [02_lsn_assignment_sequence.mermaid](about_primary_side_of_streaming_replication/diagrams/02_lsn_assignment_sequence.mermaid) | LSN assignment sequence |
| [03_wal_write_sync_flow.mermaid](about_primary_side_of_streaming_replication/diagrams/03_wal_write_sync_flow.mermaid) | WAL write and sync flow |
| [04_wal_buffer_state.mermaid](about_primary_side_of_streaming_replication/diagrams/04_wal_buffer_state.mermaid) | WAL buffer state transitions |
| [05_walsender_state.mermaid](about_primary_side_of_streaming_replication/diagrams/05_walsender_state.mermaid) | Walsender state machine |
| [06_send_data_structure.mermaid](about_primary_side_of_streaming_replication/diagrams/06_send_data_structure.mermaid) | Send data structure layout |
| [07_walsender_iteration.mermaid](about_primary_side_of_streaming_replication/diagrams/07_walsender_iteration.mermaid) | Walsender main loop iteration |
| [08_standby_response_sequence.mermaid](about_primary_side_of_streaming_replication/diagrams/08_standby_response_sequence.mermaid) | Standby response message sequence |
| [09_sync_wait_release_sequence.mermaid](about_primary_side_of_streaming_replication/diagrams/09_sync_wait_release_sequence.mermaid) | Synchronous wait/release sequence |
| [10_syncrep_queue_state.mermaid](about_primary_side_of_streaming_replication/diagrams/10_syncrep_queue_state.mermaid) | Synchronous replication queue state |
| [11_complete_commit_sequence.mermaid](about_primary_side_of_streaming_replication/diagrams/11_complete_commit_sequence.mermaid) | Complete commit-to-client sequence |

---

## Streaming Replication — Full System

A comprehensive view of PostgreSQL streaming replication across both primary and standby sides, covering process architecture, WAL transmission, walreceiver operations, startup and recovery processes, inter-process coordination, performance tuning, and debugging.

**Entry point**: [index.md](about_streaming_replication/index.md)

### Overview Documents

| File | Description |
|---|---|
| [index.md](about_streaming_replication/index.md) | Navigation index |
| [overview_and_scope.md](about_streaming_replication/overview_and_scope.md) | Scope and architecture overview |
| [streaming_replication_implementation_guide.md](about_streaming_replication/streaming_replication_implementation_guide.md) | Comprehensive implementation guide |
| [streaming_replication_performance_tuning.md](about_streaming_replication/streaming_replication_performance_tuning.md) | Performance tuning reference |
| [streaming_replication_debugging_reference.md](about_streaming_replication/streaming_replication_debugging_reference.md) | Debugging reference |

### Primary Side Processing (`primary_side_processing/`)

| File | Description |
|---|---|
| [wal_generation_to_walsender.md](about_streaming_replication/primary_side_processing/wal_generation_to_walsender.md) | WAL generation through walsender handoff |
| [walsender_transmission.md](about_streaming_replication/primary_side_processing/walsender_transmission.md) | Walsender data transmission |

### Standby Side Processing (`standby_side_processing/`)

| File | Description |
|---|---|
| [walreceiver_operations.md](about_streaming_replication/standby_side_processing/walreceiver_operations.md) | Walreceiver operations |
| [startup_decoding_process.md](about_streaming_replication/standby_side_processing/startup_decoding_process.md) | Startup process and WAL decoding |
| [startup_replay_process.md](about_streaming_replication/standby_side_processing/startup_replay_process.md) | Startup process and WAL replay |

### Inter-Process Coordination (`inter_process_coordination/`)

| File | Description |
|---|---|
| [bgwriter_integration.md](about_streaming_replication/inter_process_coordination/bgwriter_integration.md) | Background writer integration |
| [standby_feedback_protocol.md](about_streaming_replication/inter_process_coordination/standby_feedback_protocol.md) | Standby feedback protocol |

### Implementation Details (`implementation_details/`)

| File | Description |
|---|---|
| [data_structures_and_globals.md](about_streaming_replication/implementation_details/data_structures_and_globals.md) | Key data structures and global variables |
| [detailed_primary_wal_flow.md](about_streaming_replication/implementation_details/detailed_primary_wal_flow.md) | Detailed primary WAL flow |
| [detailed_walsender_processing.md](about_streaming_replication/implementation_details/detailed_walsender_processing.md) | Detailed walsender processing |
| [detailed_walreceiver_processing.md](about_streaming_replication/implementation_details/detailed_walreceiver_processing.md) | Detailed walreceiver processing |
| [detailed_startup_decoding.md](about_streaming_replication/implementation_details/detailed_startup_decoding.md) | Detailed startup decoding |
| [detailed_startup_replay.md](about_streaming_replication/implementation_details/detailed_startup_replay.md) | Detailed startup replay |
| [detailed_bgwriter_interaction.md](about_streaming_replication/implementation_details/detailed_bgwriter_interaction.md) | Detailed bgwriter interaction |
| [detailed_standby_feedback.md](about_streaming_replication/implementation_details/detailed_standby_feedback.md) | Detailed standby feedback |

### Diagrams (`about_streaming_replication/diagrams/`)

| File | Description |
|---|---|
| [walsender_state_machine.mermaid](about_streaming_replication/diagrams/walsender_state_machine.mermaid) | Walsender state machine |
| [walreceiver_data_flow.mermaid](about_streaming_replication/diagrams/walreceiver_data_flow.mermaid) | Walreceiver data flow |
| [primary_wal_flow_sequence.mermaid](about_streaming_replication/diagrams/primary_wal_flow_sequence.mermaid) | Primary WAL flow sequence |
| [bgwriter_coordination.mermaid](about_streaming_replication/diagrams/bgwriter_coordination.mermaid) | Background writer coordination |
| [inter_process_communication.mermaid](about_streaming_replication/diagrams/inter_process_communication.mermaid) | Inter-process communication |
| [startup_process_integration.mermaid](about_streaming_replication/diagrams/startup_process_integration.mermaid) | Startup process integration |
| [shared_memory_layout.mermaid](about_streaming_replication/diagrams/shared_memory_layout.mermaid) | Shared memory layout |
| [network_protocol_messages.mermaid](about_streaming_replication/diagrams/network_protocol_messages.mermaid) | Replication network protocol messages |
