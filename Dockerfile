# syntax=docker/dockerfile:1
#
# 只把「常驻的那半边」放进容器：local-client（连出去的长连接 + 本地 SSE + SQLite）。
#
# Worker 那半边故意不进来。它是 `wrangler deploy` 一条命令就结束的一次性动作，
# 塞进镜像要带 node 和几百 MB 的 node_modules，收益为零，还把 wrangler 的登录态
# 变成两个地方各存一份。部署在宿主机做，见 docs/docker.md。
#
# 镜像里装 gh 是因为「仓库列表 / 开 JIT runner」这些接口真的会调它。装法是从官方
# release 取静态二进制，不用 cli.github.com 那个 apt 源——实测那条源在国内网络下
# 12MB 的包能卡十分钟不报错，而 release 直连有 1.4MB/s。登录态不烘进镜像，
# 运行时用 GH_TOKEN（宿主机上 `gh auth token` 取）传进来。
FROM python:3.13-slim

# 钉版本：基础镜像里多一个二进制，不钉就等于每次构建都可能变
ARG GH_VERSION=2.104.0
ARG TARGETARCH
# 国内网络从 GitHub 拉可能极慢（实测 colima 里 13MB 卡十几分钟不动）。
# 换镜像站：docker compose build --build-arg GH_BASE_URL=https://<镜像站>/cli/cli/releases/download
ARG GH_BASE_URL=https://github.com/cli/cli/releases/download

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && python -c "import urllib.request as u; u.urlretrieve('${GH_BASE_URL}/v${GH_VERSION}/gh_${GH_VERSION}_linux_${TARGETARCH}.tar.gz', '/tmp/gh.tgz')" \
 && tar -xzf /tmp/gh.tgz -C /tmp \
 && install -m 0755 /tmp/gh_*/bin/gh /usr/local/bin/gh \
 && rm -rf /tmp/gh* /var/lib/apt/lists/* \
 && gh --version

WORKDIR /app
COPY local-client/ ./local-client/

# [ssh] 装上 paramiko：机器管理那套（SSH 装 runner）要靠它，容器里也想用
RUN pip install --no-cache-dir "./local-client[ssh]" \
 && useradd --create-home --uid 10001 gla \
 && mkdir -p /data && chown gla:gla /data

USER gla
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GH_NO_UPDATE_NOTIFIER=1 \
    XDG_CONFIG_HOME=/home/gla/.config

# 配置和 events.db 都放这里，挂卷才不会跟着容器一起没
VOLUME ["/data"]
EXPOSE 18091

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:18091/health', timeout=3).status == 200 else 1)"

ENTRYPOINT ["github-local-action"]
CMD ["-c", "/data/config.toml"]
