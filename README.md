# create_pg_super_document

## 概要

**create_pg_super_document** は、PostgreSQLのコードツリー内のシンボルをAIエージェントを用いて全てドキュメント化することを目指したプロジェクトです[...] このリポジトリには、主にそのためのデータ（DuckDBのDBファイル、JSONファイル）を準備し、Claude CodeなどのAIがドキュメント生成できるようにする​[...]  

## ディレクトリ・ファイル構成

- `extract_readme_file_header_comments.py` … READMEヘッダコメント抽出
- `extract_symbol_references.py` … シンボル参照関係抽出
- `import_symbol_reference.py` … シンボル参照情報のインポート
- `process_symbol_definitions.py` … シンボル定義情報の処理
- `filter_frequent_symbol_from_csv.py` … 頻出シンボルのCSVフィルタ
- `set_file_end_lines.py` … ファイル終端行の設定
- `update_symbol_types.py` … シンボル種別情報の更新
- `create_duckdb_index.py` … DuckDBインデックス作成
- `requirements.txt` … 必要パッケージ記述
- `scripts/` … 補助スクリプト群
  - `prepare_cluster.py` … シンボルのクラスタリングとAIバッチ準備（ドキュメント生成前に実行必須）
- `ENTRY_POINTS.md` … エントリポイント説明
- `GENERATION_PLAN.md` … 生成計画ドキュメント

## 必要なディレクトリの作成

本リポジトリのスクリプトを実行する前に、下記ディレクトリを作成しておくことを推奨します（既に存在する場合は不要です）。

```sh
mkdir -p data
mkdir -p scripts
mkdir -p output/temp
```

- `data/` … AIドキュメント生成バッチやDB等、各種メタデータ保存用
- `scripts/` … 補助モジュール・AI連携スクリプトの配置先
- `output/` … 生成物・一時ファイルの保存先（例：`symbol_references.csv`など）
- `output/temp/` … 一時的な中間ファイルや処理中の成果物の保存先。  
  一部のスクリプトでは`output/temp`以下を作業ディレクトリとして利用します。

各スクリプトの詳細な入出力先については、スクリプト冒頭のコメントや`GENERATION_PLAN.md`も参照してください。

---

## 推奨実行フロー

このプロジェクトのスクリプト群は、PostgreSQLコードツリーのシンボル情報をDuckDBデータベースに段階的に構築・加工し、AIエージェントによるドキュメント生成を行います。  
各スクリプトの処理対象や推奨される実行順序は以下の通りです。

### 1. シンボル定義情報の抽出・DB構築

- **create_duckdb_index.py**  
  GNU GLOBALのインデックス出力を元に、`global_symbols.db` 内に `symbol_definitions` テーブルを作成・格納します。  
  - 主なカラム:  
    - `id`（主キー）, `symbol_name`, `file_path`, `line_num_start`, `line_num_end`, `line_content`, `contents`  
  - 最初に必ず実行し、全シンボル情報をDB化します。

### 2. ファイル終端行の設定

- **set_file_end_lines.py**  
  各シンボル定義の範囲（`line_num_end`）をファイル内で設定します。  
  - 対象: `symbol_definitions` テーブル（`line_num_end` カラムを更新）

### 3. シンボル参照関係の抽出・整理

- **extract_symbol_references.py**  
  `symbol_definitions` のシンボルごとに `global -rx` を使い、参照関係を `symbol_references.csv` として出力します。

- **filter_frequent_symbol_from_csv.py**  
  頻出シンボルや不要な参照をフィルタし、`symbol_references_filtered.csv` を生成します。

- **import_symbol_reference.py**  
  `symbol_references_filtered.csv` を `global_symbols.db` の `symbol_reference` テーブルへインポートします。  
  - カラム:  
    - `from_node`（参照元シンボルID）, `to_node`（参照先シンボルID）, `line_num_in_from`

### 4. シンボル種別情報の自動付与

- **update_symbol_types.py**  
  `symbol_definitions` テーブルに `symbol_type` カラムを追加し、AI分類等で種別（関数/変数/型など）を自動推定して記録します。

### 5. シンボル定義の追加加工・重複解消

- **process_symbol_definitions.py**  
  重複定義や不要なデータの整理・統計情報出力等を行います。

### 6. シンボルのクラスタリング・バッチ準備（AIドキュメント生成前に必須）

- **scripts/prepare_cluster.py**  
  シンボル依存関係をもとに自動でクラスタ分けし、AIドキュメント生成のためのバッチ（`data/processing_batches.json` など）を準備します。  
  **このステップを必ず実行してください。**

### 7. ドキュメント生成・AI連携

- **scripts/orchestrator.py**  
  DuckDB内のシンボル情報・参照情報・クラスタ情報をもとに、Claude CodeなどのAIによる自動ドキュメント生成・管理を orchestrate（統括）します。  
  - `data/processing_batches.json` でバッチ単位の処理計画もサポート

---

## ドキュメント生成・AI連携（orchestrator.py / mcp_tool.py の役割）

- **scripts/orchestrator.py**  
  エージェントは`mcp_tool.py`を介して`output/temp`ディレクトリ内にMarkdown（.md）ファイルを作成します。  
  orchestrator.pyは、これらoutput/temp内のmdファイルの内容を抽出し、`global_symbols.db`の`documents`テーブルに追加します。これにより、AIによる各シンボルのドキュメント化が可能となります。

- **進捗管理およびログ記録**  
  orchestrator.pyによる一連のドキュメント生成・インポートの進捗や、各処理バッチの状態ログは`metadata.duckdb`に記録されます。

### 例：output/temp配下のmdファイルからdocumentsテーブルへの登録フロー

1. **AIエージェントによるMarkdown生成**  
   - mcp_tool.pyがoutput/temp/に各シンボル/ファイルごとのmdファイルを生成

2. **orchestrator.pyによるDB登録**  
   - output/temp/*.mdを探索し、内容を抽出
   - `global_symbols.db`の`documents`テーブルに内容を追加

3. **進捗記録**  
   - 各処理バッチ・ファイルの処理状況やエラー情報をmetadata.duckdbに記録

## 実行例（典型的な流れ）

```sh
# 1. シンボル定義をDB化
python create_duckdb_index.py <ソースディレクトリ>

# 2. シンボル範囲情報を補完
python set_file_end_lines.py

# 3. 参照関係の抽出とフィルタ
python extract_symbol_references.py
python filter_frequent_symbol_from_csv.py

# 4. 参照情報のDB取り込み
python import_symbol_reference.py

# 5. シンボル種別情報の付与
python update_symbol_types.py

# 6. 重複処理・最終加工
python process_symbol_definitions.py

# 7. シンボルのクラスタリングとバッチ生成（必須）
python scripts/prepare_cluster.py

# 8. AIによるドキュメント生成など
python scripts/orchestrator.py
```

### scripts/prepare_cluster.py の使い方
AIドキュメント生成のためのバッチファイル（data/processing_batches.json など）を準備します。必ずorchestrator.py実行の前にこのスクリプトを実行してください。

```sh
python scripts/prepare_cluster.py
```

## DuckDB テーブルスキーマ・データフロー概要

- **symbol_definitions**  
  → シンボル定義の主テーブル。全スクリプトの基礎データ

- **symbol_reference**  
  → シンボル間の参照関係（from_node, to_node）を記録

- **documents**  
  → AI生成によるドキュメント（`scripts/orchestrator.py`で作成・管理。output/temp内のmdファイルからインポート）

- **metadata.duckdb**  
  → ドキュメント生成・インポート進捗やバッチ処理ログを管理

その他、詳細なスキーマやデータフローは各スクリプトの先頭コメントおよび `GENERATION_PLAN.md` を参照してください。


## インストール・セットアップ

1. Python 3.x をインストールしてください（詳細バージョンは `python_version` ファイルを参照）
2. 必要なパッケージをインストールします:
   ```sh
   pip install -r requirements.txt
   ```

## 使い方
主な解析・加工スクリプトはコマンドラインから実行できます。  
詳細な使い方やオプションは各スクリプトの先頭コメントや `ENTRY_POINTS.md` を参照してください。

## 依存パッケージ
- `requirements.txt` に記載

## 前提とするPostgrSQLのコードツリー
- https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614
- 別のコードツリーでも問題ないと思いますが、リポジトリに登録してあるDBファイル、csvファイルなどはこのコードツリーに対応したものになっています。

## 生成例
- https://gist.github.com/ryogrid/af4c9ce3fb89a9f196ecd2e2109b8fc6


## 関連資料
- [GENERATION_PLAN.md](./GENERATION_PLAN.md): 生成計画詳細

