"""dbt 実行ラッパー。

BigQuery Emulator 用の monkey patch を適用してから dbt CLI を実行する。
"""

import sys

# dbt 起動前にパッチを適用
import bq_emulator_patch

bq_emulator_patch.apply()

# dbt CLI のメインエントリポイントを実行
from dbt.cli.main import cli

cli()
