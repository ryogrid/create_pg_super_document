# create_pg_super_document

## Overview

**create_pg_super_document** is a project aiming to document all symbols in the PostgreSQL code tree using AI agents. This repository mainly prepares the necessary data (DuckDB DB files, JSON files, CSV files, etc.) from the PostgreSQL source tree (function/variable/type definitions, reference relationships, symbol clustering), and supplies them to AI agents (e.g., Claude, OpenAI, etc.) to automatically generate high‑quality documentation.

## Directory & File Structure

- `extract_readme_file_header_comments.py` ... Extract README header comments
- `extract_symbol_references.py` ... Extract symbol reference relationships
- `import_symbol_reference.py` ... Import symbol reference information
- `process_symbol_definitions.py` ... Process symbol definition information
- `filter_frequent_symbol_from_csv.py` ... Filter frequent symbols in CSV
- `set_file_end_lines.py` ... Set file end lines
- `update_symbol_types.py` ... Update symbol type information
- `create_duckdb_index.py` ... Create DuckDB index
- `requirements.txt` ... Required packages
- `scripts/` ... Auxiliary scripts
  - `prepare_cluster.py` ... Symbol clustering and AI batch preparation (must be run before documentation generation)
  - `orchestrator.py` ... High-level coordination of AI documentation generation
  - `mcp_tool.py` ... Tool interface (Model Context Protocol) used by the AI agent to retrieve context and persist generated documents
- `ENTRY_POINTS.md` ... Entry point explanations
- `GENERATION_PLAN.md` ... Generation plan document

## Required Directory Creation

Before running the scripts in this repository, it is recommended to create the following directories (skip if they already exist):

```sh
mkdir -p data
mkdir -p scripts
mkdir -p output/temp
```

 - `data/` ... For storing AI documentation generation batches, DBs, and various metadata
 - `scripts/` ... Location for auxiliary modules and AI integration scripts
 - `output/` ... Storage for generated and temporary files (e.g., `symbol_references.csv`)
 - `output/temp/` ... Storage for temporary intermediate files and in-process results.  
   Some scripts use `output/temp` as a working directory.

For details on input/output locations for each script, see the comments at the top of each script or refer to `GENERATION_PLAN.md`.

---

## GNU GLOBAL Installation and Usage (Symbol Index Generation)

In the initial stage of this project, we build symbol definition and reference indices from the PostgreSQL source tree using GNU GLOBAL (gtags). The resulting data is then imported and refined into DuckDB.  
Follow the steps below (do this before running the Python scripts).

### 1. Install GNU GLOBAL

Common installation methods:

#### macOS (Homebrew)

```sh
brew install global
```

#### Debian / Ubuntu

```sh
sudo apt-get update
sudo apt-get install -y global exuberant-ctags
```

(If you already use universal-ctags instead of exuberant-ctags, that is fine.)

#### From Source

```sh
wget https://ftp.gnu.org/pub/gnu/global/global-latest.tar.gz
tar xf global-latest.tar.gz
cd global-*/
./configure --prefix=/usr/local
make -j$(nproc)
sudo make install
global --version
```

### 2. Obtain the PostgreSQL Source Tree

(If already cloned, skip.)

```sh
git clone https://github.com/postgres/postgres.git
cd postgres
# Checkout the specific commit used for the data included in this repository (optional)
git checkout 92268b35d04c2de416279f187d12f264afa22614
```

### 3. Create GNU GLOBAL Index Files

Run `gtags` at the root of the PostgreSQL source directory.

```sh
cd /path/to/postgres
gtags --gtagslabel=ctags
```

Generated key files:
- `GTAGS`   : Definitions database
- `GRTAGS`  : References database
- `GPATH`   : Path mapping

Incremental update after source changes:

```sh
gtags --incremental
```

If you manage multiple branches or trees on the same machine, you can isolate object directories:

```sh
export GTAGSOBJDIRPREFIX=$HOME/.gtags/objs
gtags
```

(Each source root will then get its own object directory automatically.)

### 4. Basic Usage of the `global` Command

`global` queries the index to locate symbol definitions and references.

Frequently used options:
- Definitions: `global -d SYMBOL`
- References (callers, usages): `global -r SYMBOL`
- Cross reference with line numbers: `global -x SYMBOL`
- Partial/substring match: `global -c PATTERN` or `global -s PATTERN`
- List symbols defined in a file: `global -f path/to/file.c`
- Regex match across tags: `global -a -e 'Exec[A-Za-z0-9_]+'`
- Detailed reference lines (used in this repo): `global -rx SYMBOL`

Examples:

```sh
# Definition of heap_insert
global -x heap_insert

# All places referencing ExecProcNode
global -rx ExecProcNode

# List all symbols in parse_expr.c
global -f src/backend/parser/parse_expr.c
```

### 5. Mapping to This Repository's Scripts

| Script | Uses GNU GLOBAL Output | Prerequisite |
|--------|------------------------|--------------|
| `create_duckdb_index.py` | `GTAGS` (definitions) | Run `gtags` in PostgreSQL root beforehand |
| `extract_symbol_references.py` | `global -rx SYMBOL` requiring `GTAGS/GRTAGS/GPATH` | Same |
| `filter_frequent_symbol_from_csv.py` | Processes CSV produced upstream | Upstream CSV exists |
| `import_symbol_reference.py` | Filtered reference CSV | DuckDB DB + CSV |
| Other update/processing scripts | DuckDB tables | `create_duckdb_index.py` completed |

When running `create_duckdb_index.py`, pass the PostgreSQL source directory path as the argument. That directory must already contain the generated `GTAGS` files.

### 6. Troubleshooting

| Symptom | Possible Cause | Resolution |
|---------|----------------|-----------|
| `global: GTAGS not found.` | Index not created | Run `gtags` at the source root |
| Reference results empty | Macro expansion / alternative symbol name | Try related identifiers / adjust label / verify commit |
| Results outdated | Source changed without reindex | Run `gtags --incremental` or regenerate |
| High memory usage | Full tree reindex repeatedly | Limit scope (invoke in subset), or ensure adequate RAM |

---

## Recommended Execution Flow

The scripts in this project incrementally build and process symbol information from the PostgreSQL code tree into a DuckDB database, then generate documentation using AI agents.  
The processing targets and recommended execution order for each script are as follows:

### 1. Extract Symbol Definition Information & Build DB

- **create_duckdb_index.py**  
  Based on the index output from GNU GLOBAL, creates and stores the `symbol_definitions` table in `global_symbols.db`.  
  - Main columns:  
    - `id` (primary key), `symbol_name`, `file_path`, `line_num_start`, `line_num_end`, `line_content`, `contents`  
  - Must be run first to import all symbol information into the DB.

### 2. Set File End Lines

- **set_file_end_lines.py**  
  Sets the range (`line_num_end`) for each symbol definition within the file.  
  - Target: `symbol_definitions` table (updates the `line_num_end` column)

### 3. Extract & Organize Symbol Reference Relationships

- **extract_symbol_references.py**  
  Uses `global -rx` for each symbol in `symbol_definitions` to output reference relationships as `symbol_references.csv`.

- **filter_frequent_symbol_from_csv.py**  
  Filters frequent symbols and unnecessary references to generate `symbol_references_filtered.csv`.

- **import_symbol_reference.py**  
  Imports `symbol_references_filtered.csv` into the `symbol_reference` table in `global_symbols.db`.  
  - Columns:  
    - `from_node` (source symbol ID), `to_node` (target symbol ID), `line_num_in_from`

### 4. Automatic Assignment of Symbol Type Information

- **update_symbol_types.py**  
  Adds a `symbol_type` column to the `symbol_definitions` table and automatically estimates and records the type (function/variable/type, etc.) .

### 5. Additional Processing & Deduplication of Symbol Definitions

- **process_symbol_definitions.py**  
  Cleans up duplicate definitions and unnecessary data, outputs statistics, etc.

### 6. Symbol Clustering & Batch Preparation (Required Before AI Documentation Generation)

- **scripts/prepare_cluster.py**  
  Automatically clusters symbols based on dependencies and prepares batches (e.g., `data/processing_batches.json`) for AI documentation generation.  
  **Be sure to run this step.**

### 7. Documentation Generation & AI Integration

- **scripts/orchestrator.py**  
  Orchestrates automatic documentation generation and management by AI (e.g., Claude Code) based on symbol, reference, and cluster information in DuckDB.  
  - Also supports batch processing plans via `data/processing_batches.json`

---

## Documentation Generation & AI Integration (Roles of orchestrator.py / mcp_tool.py)

- **scripts/orchestrator.py**  
  The agent creates Markdown (.md) files in the `output/temp` directory via `mcp_tool.py`.  
  orchestrator.py extracts the contents of these md files in output/temp and adds them to the `documents` table in `global_symbols.db`, enabling AI-based documentation for each symbol.

- **scripts/mcp_tool.py (Tool Interface for the AI Agent)**  
  `mcp_tool.py` implements the tool-side interface (e.g., Model Context Protocol style) that the AI agent invokes to obtain exactly the contextual information required to write a symbol’s documentation. Its responsibilities typically include:  
  - Fetching symbol definition records (name, file path, source snippet, start/end lines) from the DuckDB database.  
  - Supplying reference relationships (incoming/outgoing dependencies) so the agent can describe usage patterns.  
  - Providing cluster/batch metadata (e.g., related symbols processed together) to encourage consistent narrative across a group of symbols.  
  - Returning any existing previously generated documents (for incremental refinement or avoidance of duplication).  
  - Accepting newly generated Markdown content from the agent and writing it to `output/temp/*.md` so `orchestrator.py` can ingest it into the `documents` table.  
  - (Optionally) Logging tool invocations or marking progress so that interrupted runs can resume.  

  Conceptual interaction flow:  
  1. Orchestrator selects next symbol (or batch) from planning data (`data/processing_batches.json`).  
  2. AI agent requests context via exposed tool methods in `mcp_tool.py` (e.g., get_symbol_definition, get_references, get_cluster_context).  
  3. AI agent generates a Markdown document and calls a write/persist method in `mcp_tool.py` to store it under `output/temp/`.  
  4. Orchestrator later scans `output/temp/` and imports finalized content into DuckDB.  

  This separation keeps the agent stateless regarding storage details, while `mcp_tool.py` abstracts data retrieval and persistence. If you extend documentation scope (e.g., add performance notes, code metrics), you can augment `mcp_tool.py` with new retrieval endpoints without modifying the orchestration logic.

- **Progress Management and Logging**  
  Progress of documentation generation/import and batch status logs by `orchestrator.py` are recorded in `metadata.duckdb`.

### Example: Registration Flow from md Files under output/temp to the documents Table

1. **Markdown Generation by AI Agent**  
   - `mcp_tool.py` provides context; the agent creates md files for each symbol/file in `output/temp/`
2. **DB Registration by orchestrator.py**  
   - Searches for `output/temp/*.md`, extracts contents
   - Adds contents to the `documents` table in `global_symbols.db`
3. **Progress Logging**  
   - Records processing status and error information for each batch/file in `metadata.duckdb`

## Example Execution (Typical Flow)

```sh
# 0. (Prerequisite) Prepare GNU GLOBAL index inside PostgreSQL source tree
#    cd /path/to/postgres && gtags --gtagslabel=ctags

# 1. Import symbol definitions into DB
python create_duckdb_index.py <source_directory>

# 2. Complete symbol range information
python set_file_end_lines.py

# 3. Extract and filter reference relationships
python extract_symbol_references.py
python filter_frequent_symbol_from_csv.py

# 4. Import reference information into DB
python import_symbol_reference.py

# 5. Assign symbol type information
python update_symbol_types.py

# 6. Deduplication and final processing
python process_symbol_definitions.py

# 7. Symbol clustering and batch generation (required)
python scripts/prepare_cluster.py

# 8. AI-based documentation generation (tool-assisted)
python scripts/orchestrator.py
```

### How to Use scripts/prepare_cluster.py
Prepares batch files (such as `data/processing_batches.json`) for AI documentation generation. Be sure to run this script before executing `orchestrator.py`.

```sh
python scripts/prepare_cluster.py
```

## DuckDB Table Schema & Data Flow Overview

- **symbol_definitions**  
  → Main table for symbol definitions. Base data for all scripts
- **symbol_reference**  
  → Records reference relationships between symbols (from_node, to_node)
- **documents**  
  → Documentation generated by AI (created/managed by `scripts/orchestrator.py`, imported from md files in `output/temp`)
- **metadata.duckdb**  
  → Manages documentation generation/import progress and batch processing logs

For more detailed schema and data flow, see the comments at the top of each script and `GENERATION_PLAN.md`.

## Installation & Setup

1. Install Python 3.x (see the `python_version` file for the specific version)
2. Install required packages:
  ```sh
  pip install -r requirements.txt
  ```

(Install GNU GLOBAL earlier as described above if not yet installed.)

## Usage
The main analysis and processing scripts can be run from the command line.  
For detailed usage and options, see the comments at the top of each script or refer to `ENTRY_POINTS.md`.

## Dependencies
- Listed in `requirements.txt`

## Assumed PostgreSQL Code Tree
- https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614
- Other code trees should also work, but the DB and CSV files registered in this repository correspond to this code tree.

## Example Output
- https://gist.github.com/ryogrid/af4c9ce3fb89a9f196ecd2e2109b8fc6

## Related Materials
- [GENERATION_PLAN.md](./GENERATION_PLAN.md): Generation plan details
