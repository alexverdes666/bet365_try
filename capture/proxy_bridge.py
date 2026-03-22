"""
Local proxy bridge: accepts unauthenticated SOCKS5 connections locally
and forwards them to the remote authenticated SOCKS5 proxy.

pproxy URI format uses # for auth: socks5://host:port#username:password
"""

import asyncio

import pproxy


async def run_bridge():
    server = pproxy.Server("socks5://127.0.0.1:1089")
    remote = pproxy.Connection("socks5://188.126.20.21:8081#px95358f:4hjzQNheHNjtz7vTXcPwEyxE")
    handler = await server.start_server({"rserver": [remote]})
    import sys
    print("PROXY_READY", flush=True)
    print("Proxy bridge running on socks5://127.0.0.1:1089 -> remote SOCKS5", file=sys.stderr)

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("Stopping proxy bridge...")


if __name__ == "__main__":
    asyncio.run(run_bridge())
