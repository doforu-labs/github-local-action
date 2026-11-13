#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把本机做成本局域网的出口代理（HTTP/HTTPS 都走 CONNECT）。

两种模式：
  --upstream 127.0.0.1:7897   转发到本机已有的代理（Clash 之类），一个字都不解析；
  --direct                    自己直连目标（用本机的外网线路），不经过任何代理。

为什么要有 --direct：实测本机直连 github.com 有 1 MB/s，而走本机 Clash 只有
48~183 KB/s（Clash 的规则把 github 甩到了慢节点）。自建 runner 的机器常常只有
60~100 KB/s，让它们借本机的直连线路比自己直连快十倍。

安全：默认只接受内网来源（--allow），并把本机回环排在外面。
用法：
    python3 lan_proxy.py --listen 0.0.0.0:18092 --direct --allow 192.168.0.0/16
"""
from __future__ import annotations

import argparse
import ipaddress
import socket
import sys
import threading

BUFSIZE = 65536
HEAD_MAX = 65536


def pipe(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(BUFSIZE)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass


def _read_head(sock: socket.socket) -> bytes:
    """读到请求头结束（\\r\\n\\r\\n）。返回原始字节，最多 HEAD_MAX。"""
    buf = b""
    while b"\r\n\r\n" not in buf and len(buf) < HEAD_MAX:
        chunk = sock.recv(BUFSIZE)
        if not chunk:
            break
        buf += chunk
    return buf


def _split_hostport(authority: str, default_port: int) -> tuple[str, int] | None:
    authority = authority.strip()
    if not authority:
        return None
    if authority.startswith("["):                     # IPv6 字面量
        host, _, rest = authority[1:].partition("]")
        port = rest.lstrip(":")
        return (host, int(port)) if port.isdigit() else (host, default_port)
    if ":" in authority:
        host, _, port = authority.rpartition(":")
        if port.isdigit():
            return host, int(port)
        return None
    return authority, default_port


def _connect_direct(host: str, port: int) -> socket.socket:
    return socket.create_connection((host, port), timeout=15)


def handle_direct(client: socket.socket, addr: tuple, log) -> None:
    """自己直连：解析请求行，CONNECT 就隧道，普通 HTTP 就改写请求行再转发。"""
    try:
        head = _read_head(client)
        if not head:
            client.close()
            return
        line_end = head.find(b"\r\n")
        first = head[:line_end].decode("latin-1") if line_end > 0 else head.decode("latin-1")
        parts = first.split()
        if len(parts) < 3:
            client.close()
            return
        method, target, version = parts[0], parts[1], parts[2]

        if method.upper() == "CONNECT":
            hp = _split_hostport(target, 443)
            if not hp:
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                client.close()
                return
            up = _connect_direct(*hp)
            client.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
            threading.Thread(target=pipe, args=(client, up), daemon=True).start()
            pipe(up, client)
            log(f"CONNECT {hp[0]}:{hp[1]} <- {addr[0]}")
            return

        # 普通 HTTP：客户端发给代理的是绝对地址，要换成 origin-form
        if target.startswith("http://"):
            rest = target[7:]
            slash = rest.find("/")
            authority = rest if slash < 0 else rest[:slash]
            path = "/" if slash < 0 else rest[slash:]
            hp = _split_hostport(authority, 80)
            if not hp:
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                client.close()
                return
            body_start = head.find(b"\r\n\r\n") + 4
            new_head = f"{method} {path} {version}\r\n".encode("latin-1") + head[line_end + 2:body_start]
            up = _connect_direct(*hp)
            up.sendall(new_head + head[body_start:])
            threading.Thread(target=pipe, args=(client, up), daemon=True).start()
            pipe(up, client)
            log(f"{method} {hp[0]}:{hp[1]}{path[:40]} <- {addr[0]}")
            return

        client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        client.close()
    except OSError as exc:
        log(f"直连失败 {addr[0]}: {exc}")
        try:
            client.close()
        except OSError:
            pass


def handle_relay(client: socket.socket, addr: tuple, upstream: tuple[str, int], log) -> None:
    """纯字节转发到上游代理（上游自己会认 HTTP / CONNECT / SOCKS）。"""
    try:
        up = socket.create_connection(upstream, timeout=15)
    except OSError as exc:
        log(f"上游连不上（{addr[0]}）: {exc}")
        try:
            client.close()
        except OSError:
            pass
        return
    up.settimeout(None)
    client.settimeout(None)
    threading.Thread(target=pipe, args=(client, up), daemon=True).start()
    pipe(up, client)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default="0.0.0.0:18092")
    ap.add_argument("--upstream", default="127.0.0.1:7897")
    ap.add_argument("--direct", action="store_true", help="自己直连，不转发到上游代理")
    ap.add_argument("--allow", default="192.168.0.0/16",
                    help="允许来源网段，逗号分隔；空字符串=不限制")
    args = ap.parse_args()

    lh, lp = args.listen.rsplit(":", 1)
    uh, up_port = args.upstream.rsplit(":", 1)
    nets = []
    if args.allow.strip():
        for part in args.allow.split(","):
            part = part.strip()
            if part:
                nets.append(ipaddress.ip_network(part, strict=False))

    def log(text: str) -> None:
        print(text, flush=True)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((lh, int(lp)))
    srv.listen(128)
    mode = "直连" if args.direct else f"转发到 {uh}:{up_port}"
    log(f"出口代理已起：{lh}:{lp}  模式={mode}  允许来源={[str(n) for n in nets] or '不限'}")

    while True:
        try:
            conn, addr = srv.accept()
        except KeyboardInterrupt:
            break
        if nets:
            try:
                ip = ipaddress.ip_address(addr[0])
            except ValueError:
                conn.close()
                continue
            if not any(ip in n for n in nets):
                log(f"拒绝 {addr[0]}（不在允许网段）")
                conn.close()
                continue
        conn.settimeout(None)
        if args.direct:
            threading.Thread(target=handle_direct, args=(conn, addr, log), daemon=True).start()
        else:
            threading.Thread(target=handle_relay,
                             args=(conn, addr, (uh, int(up_port)), log), daemon=True).start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
