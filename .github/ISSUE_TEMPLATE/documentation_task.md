---
name: "Run Documentation Generation Agent"
title: "Agent Task: Document All Remaining PostgreSQL Symbols"
labels: ["documentation", "copilot-task"]
assignees: ''
---

**Objective**: Autonomously document all remaining unprocessed symbols in the PostgreSQL codebase.

@github-copilot, please perform the following workflow:

Your primary working branch is `copilot`. You will perform all tasks on this branch. However, to persist the database state, you will temporarily switch to the `agent-documentation-progress` branch, commit, and push, then immediately return to the `copilot` branch to continue your work.

### Your Autonomous Workflow:

1.  **Initial Setup**:
    *   First, ensure you are on your designated working branch, `copilot`.
        ```bash
        git fetch origin
        git checkout -b copilot origin/copilot || git checkout copilot
        git reset --hard origin/copilot # Ensure it's clean and up-to-date
        ```
    *   Next, run the setup script to prepare the environment. This only needs to be done once.
        ```bash
        ./scripts/setup_environment.sh
        ```

2.  **Start the Processing Loop**: Execute the following steps repeatedly on the `copilot` branch until no more batches are left.

    a. **Get Next Task with Context**: Run the script to get the next batch and save its context to a file.
       ```bash
       python3 scripts/get_next_batch.py > current_batch.json
       ```

    b. **Check for Completion**: Examine `current_batch.json`. If it indicates all batches are processed, your main task is complete. Proceed to the "Finalize" step.

    c. **Generate and Save Documents**: For each symbol in `current_batch.json`, generate the documentation and save it using the `mcp_tool`.
       ```bash
       python3 scripts/mcp_tool.py return_document [symbol_name] "[Generated Markdown Content]"
       ```

    d. **Ingest and Finalize Batch**: Ingest the generated markdown files into the local DuckDB database.
       ```bash
       python3 scripts/ingest_documents.py
       ```

    e. **!!! IMPORTANT: Persist Your Work to the Data Branch !!!**:
       *   **Step 1: Switch to the data branch.** Create it if it doesn't exist, based on the `copilot` branch.
           ```bash
           git fetch origin
           git checkout -b agent-documentation-progress origin/copilot || git checkout agent-documentation-progress
           git reset --hard origin/copilot
           ```
       *   **Step 2: Copy the updated database from your working branch.** The `git checkout <branch> -- <file>` command is perfect for this.
           ```bash
           git checkout copilot -- data/documents.duckdb
           ```
       *   **Step 3: Commit and push the database.**
           ```bash
           git config --global user.name "GitHub Copilot Agent"
           git config --global user.email "copilot-agent@users.noreply.github.com"
           git add data/documents.duckdb
           BATCH_ID=$(jq -r '.batch_id' ../current_batch.json) # Note: Path changes due to checkout
           git commit -m "docs(data): Persist documentation from batch ${BATCH_ID}"
           git push --set-upstream origin agent-documentation-progress
           ```
       *   **Step 4: Return to your working branch.** This is crucial.
           ```bash
           git checkout copilot
           ```
       *   **Step 5: Clean up the temporary file.**
           ```bash
           rm current_batch.json
           ```

3.  **Finalize**: Once the loop is complete, create a pull request from the `agent-documentation-progress` branch to `copilot`.