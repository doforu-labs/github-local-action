# Docker

[English](docker.md)

只把「常驻的那半边」放进容器：`local-client`（连出去的长连接 + 本地 SSE + SQLite）。Worker 故意不放进去——它部署时就一条 `wrangler deploy`，塞进镜像要背 node 和几百 MB 的 `node_modules`，收益为零，还会让你的 wrangler 登录态变成两处各存一份。

## 快速开始

```bash
export GH_TOKEN=$(gh auth token)     # 可选：复用本机已有的 gh 登录
mkdir -p data
cp local-client/config.example.toml data/config.toml   # 填 ws_url 和 token
docker compose up -d --build
docker compose ps                    # 等到 healthy
curl -s http://127.0.0.1:18091/health
```

然后打开 <http://127.0.0.1:18091>，状态页就在那里。Worker 要先在宿主机上部署好（见下文），否则中继没有可连的地方。

如果你那边从 GitHub 拉 gh 很慢（这里从容器网络拉它卡了十几分钟没动），换镜像站：

```bash
docker compose build --build-arg GH_BASE_URL=https://<你的镜像站>/cli/cli/releases/download
```

`data/` 里会出现 `config.toml` 和 `events.db`。备份这一个目录就等于备份了全部。

Linux 上 `./data` 要让容器内的 uid 10001 能写：

```bash
sudo chown -R 10001:10001 ./data
```

macOS / Windows 的 Docker Desktop 会自己处理，不用管。

## 为什么配置里要写 `trusted_peers = ["gateway"]`

所有写接口（改配置、管机器、调 GitHub）只答应「跑中继的那台机器」发来的请求，判据是来源地址等于 `127.0.0.1`。容器里，宿主机上浏览器过来的地址是 docker 网桥网关（`172.17.0.1` 之类），不是回环——于是页面能打开、数据都能读，但每一个「保存」类按钮都返回 403。

`trusted_peers` 就是「这些地址也算这台机器」。写特殊值 `gateway` 会让程序启动时自己从 `/proc/net/route` 读默认路由，不用你手查网桥地址。compose 里用 `GLA_TRUSTED_PEERS=gateway` 设它。

这个名单只能写在配置文件里，不开放给 HTTP 接口——名单能被人从请求里改的闸，等于没有闸。同网段的其他设备写什么都不放行。

## 复用本机已有的 gh

跟 GitHub 有关的接口（仓库列表、开 JIT runner）是直接调 `gh` 的。容器里 `gh` 有，但没登录。最干净的补法是给令牌：

```bash
export GH_TOKEN=$(gh auth token)
docker compose up -d
```

这样容器里的 `gh api` 完全不需要配置目录就能用——这条实测过。别把令牌烤进镜像，也别写进 compose 文件，那是活凭据。

想挂配置目录也行，但 `~/.config/gh/hosts.yml` 是 `600` 且属于你本人，容器里跑的是 uid 10001：macOS 的挂载通常能读过去，Linux 上会直接权限拒绝。给令牌省事得多。

## 复用本机已有的 wrangler

宿主机上的**二进制**没法复用：容器里得是另一个 OS 的另一份 node 与全局 `wrangler`。能复用而且真正值得复用的是**登录态**——办法就是干脆不把 wrangler 放进容器：

```bash
cd remote-worker
npx wrangler deploy
```

在宿主机上跑，用的就是你已有的 wrangler 登录，这是推荐路径。由此有两个直接后果：

- 状态页那个「一键部署」按钮会提示没装 wrangler。容器里这是预期行为，不是坏功能。
- 镜像里也没有 `remote-worker/` 目录，所以就算装了 wrangler，一键部署也没有 `wrangler.toml` 可读。真要在容器里一键部署，得自己派生一个带 node + wrangler 的镜像，再把 `remote-worker/` 拷进去——到那一步镜像会大几百 MB，而且 Cloudflare 凭据又多了一处存放地。

部署完，把拿到的 `wss://...workers.dev/ws` 和令牌写进 `data/config.toml`，重启容器即可。

## 当成服务常驻

容器里别点「安装为服务」——那个按钮写的是 launchd / systemd 单元文件，在容器里没有意义。compose 里的 `restart: unless-stopped` 就是容器版的「开机自启」。

## 已知边界

- **没有跑 job 时的实时日志。** 容器里也一样，原因是设计上没有日志流。见 [known-limitations.zh-CN.md](known-limitations.zh-CN.md)。
- **局域网能读。** 映射 `18091` 之后，同网段设备能看事件流和状态页——这是设计如此，写操作仍然只认本机。想让容器只做局域网读服务，`trusted_peers` 保持不动就行。
- **macOS 别用 `network_mode: host`。** 它的行为跟 Linux 不一样，而且用它等于绕回「靠回环地址判定」，正是 `trusted_peers` 想避免的那件事。
- **容器管的机器靠 SSH 从容器内发起**，所以那些机器的地址要能从容器网络够到，不能只在宿主机的终端里通。
