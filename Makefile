.PHONY: up down restart build logs ps \
       psql bq dbt-run dbt-test dbt-build \
       el-rerun dbt-rerun ui \
       clean status help

# ==============================================================================
# Core
# ==============================================================================

up: ## 全サービスを起動（初回はビルド込み）
	docker compose up --build --wait

down: ## 全サービスを停止しデータも削除
	docker compose down -v

restart: down up ## クリーンスタート（down → up）

build: ## イメージを再ビルド（起動はしない）
	docker compose build

# ==============================================================================
# Status / Logs
# ==============================================================================

ps: ## コンテナの状態を表示
	docker compose ps -a

logs: ## 全サービスのログを表示
	docker compose logs

logs-el: ## EL サービスのログを表示
	docker compose logs el

logs-dbt: ## dbt サービスのログを表示
	docker compose logs dbt

status: ## dbt の実行結果サマリーを表示
	@docker compose logs dbt 2>&1 | grep -E "(PASS=|ERROR=|Finished|Found)" | tail -4

# ==============================================================================
# Database Access
# ==============================================================================

psql: ## PostgreSQL に接続
	docker compose exec postgres psql -U postgres -d ecsite

bq: ## BigQuery Emulator にクエリを実行 (usage: make bq Q="SELECT ...")
	@./scripts/bqquery.sh --table $(Q)

bq-json: ## BigQuery Emulator にクエリを実行（JSON 出力）
	@./scripts/bqquery.sh $(Q)

bq-csv: ## BigQuery Emulator にクエリを実行（CSV 出力）
	@./scripts/bqquery.sh --csv $(Q)

# ==============================================================================
# EL / dbt 個別実行
# ==============================================================================

el-rerun: ## EL パイプラインを再実行
	docker compose run --rm el

dbt-run: ## dbt run を実行
	docker compose run --rm dbt python run_dbt.py run

dbt-test: ## dbt test を実行
	docker compose run --rm dbt python run_dbt.py test

dbt-build: ## dbt build（run + test）を実行
	docker compose run --rm dbt python run_dbt.py build

# ==============================================================================
# Analysis
# ==============================================================================

analysis: ## 分析クエリ（先頭）を実行
	@./scripts/bqquery.sh --table queries/analysis.sql

tables: ## dwh データセットの全テーブル一覧を表示
	@curl -s http://localhost:9050/bigquery/v2/projects/poc-project/datasets/dwh/tables \
		| jq -r '.tables[].tableReference.tableId' | sort

# ==============================================================================
# Web UI
# ==============================================================================

ui: ## データ確認用の Web UI を起動 (http://localhost:3000)
	docker compose --profile ui up --build -d ui
	@echo "\n  → http://localhost:3000\n"

ui-down: ## Web UI を停止
	docker compose --profile ui stop ui

# ==============================================================================
# Cleanup
# ==============================================================================

clean: down ## コンテナ・ボリューム・ビルドキャッシュを削除
	docker compose rm -f
	docker image prune -f

# ==============================================================================
# Help
# ==============================================================================

help: ## このヘルプを表示
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
