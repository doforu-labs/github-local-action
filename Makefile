# 一条命令能做的事。python3 脚本跨平台，make 只是个别名。
#
# 解释器可以用 PYTHON 覆盖：有些环境里 `python3` 不是装了依赖的那个（比如
# venv 目录被挪过、或机器上同时有 homebrew python 和 pyenv python）：
#   make test PYTHON=.venv/bin/python
PYTHON ?= python3

.PHONY: help test test-fast test-worker typecheck pii shot clean

help:
	@echo "make test        跑全部测试（17 个单测 + e2e，约 1 分钟）"
	@echo "make test-fast   只跑单测，不跑 e2e"
	@echo "make test-worker Worker 的 TS 类型检查"
	@echo "make pii         扫仓库里有没有不该公开的邮箱 / 内网 IP / 令牌"
	@echo "make shot        重新截 docs/images 里的界面图（需要本地跑着一个接收端）"
	@echo ""
	@echo "python3 不是装了依赖的那个时：make test PYTHON=.venv/bin/python"

test:
	$(PYTHON) scripts/run_tests.py

test-fast:
	$(PYTHON) scripts/run_tests.py --no-e2e

typecheck test-worker:
	cd remote-worker && npm run typecheck

pii:
	$(PYTHON) scripts/check_pii.py

clean:
	rm -rf local-client/__pycache__ local-client/tests/__pycache__
	find . -name '*.pyc' -delete
