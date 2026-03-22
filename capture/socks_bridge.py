"""
Minimal SOCKS5 proxy bridge using raw asyncio sockets.
No pproxy dependency - just forwards connections.

Local: socks5://127.0.0.1:1089 (no auth)
Remote: socks5://188.126.20.21:8081 (with auth)
"""
import asyncio
import struct
import sys

REMOTE_HOST = "188.126.20.21"
REMOTE_PORT = 8081
REMOTE_USER = b"px95358f"
REMOTE_PASS = b"4hjzQNheHNjtz7vTXcPwEyxE"
LOCAL_PORT = 1089


async def socks5_handshake(remote_r, remote_w, dest_host: bytes, dest_port: int):
    """Do SOCKS5 handshake with the remote proxy."""
    # Greeting: version=5, 1 method (user/pass=0x02)
    remote_w.write(b"\x05\x01\x02")
    await remote_w.drain()
    resp = await remote_r.readexactly(2)
    if resp[1] != 0x02:
        raise Exception(f"Remote rejected auth method: {resp}")

    # Auth: version=1, user, pass
    remote_w.write(
        b"\x01"
        + bytes([len(REMOTE_USER)]) + REMOTE_USER
        + bytes([len(REMOTE_PASS)]) + REMOTE_PASS
    )
    await remote_w.drain()
    resp = await remote_r.readexactly(2)
    if resp[1] != 0x00:
        raise Exception("Remote auth failed")

    # Connect request
    remote_w.write(
        b"\x05\x01\x00\x03"
        + bytes([len(dest_host)]) + dest_host
        + struct.pack("!H", dest_port)
    )
    await remote_w.drain()
    resp = await remote_r.readexactly(4)
    if resp[1] != 0x00:
        raise Exception(f"Remote connect failed: {resp[1]}")
    # Read remaining response (address)
    if resp[3] == 0x01:  # IPv4
        await remote_r.readexactly(4 + 2)
    elif resp[3] == 0x03:  # Domain
        alen = (await remote_r.readexactly(1))[0]
        await remote_r.readexactly(alen + 2)
    elif resp[3] == 0x04:  # IPv6
        await remote_r.readexactly(16 + 2)


async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (asyncio.CancelledError, ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def handle_client(client_r, client_w):
    try:
        # SOCKS5 greeting from local client (no auth)
        data = await asyncio.wait_for(client_r.readexactly(2), timeout=10)
        nmethods = data[1]
        await client_r.readexactly(nmethods)
        client_w.write(b"\x05\x00")  # no auth
        await client_w.drain()

        # Connect request from local client
        data = await asyncio.wait_for(client_r.readexactly(4), timeout=10)
        if data[1] != 0x01:
            client_w.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
            await client_w.drain()
            return

        # Read destination
        atype = data[3]
        if atype == 0x01:  # IPv4
            dest_addr = await client_r.readexactly(4)
            dest_host = ".".join(str(b) for b in dest_addr).encode()
        elif atype == 0x03:  # Domain
            alen = (await client_r.readexactly(1))[0]
            dest_host = await client_r.readexactly(alen)
        elif atype == 0x04:  # IPv6
            dest_addr = await client_r.readexactly(16)
            dest_host = dest_addr.hex().encode()
        else:
            return
        dest_port = struct.unpack("!H", await client_r.readexactly(2))[0]

        # Connect to remote SOCKS5 proxy
        remote_r, remote_w = await asyncio.wait_for(
            asyncio.open_connection(REMOTE_HOST, REMOTE_PORT), timeout=10
        )

        # Do SOCKS5 handshake with remote
        await socks5_handshake(remote_r, remote_w, dest_host, dest_port)

        # Send success to local client
        client_w.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
        await client_w.drain()

        # Pipe data both ways
        await asyncio.gather(
            pipe(client_r, remote_w),
            pipe(remote_r, client_w),
        )
    except Exception:
        pass
    finally:
        try:
            client_w.close()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle_client, "127.0.0.1", LOCAL_PORT)
    print("BRIDGE_READY", flush=True)
    print(f"SOCKS5 bridge on 127.0.0.1:{LOCAL_PORT} -> {REMOTE_HOST}:{REMOTE_PORT}", file=sys.stderr)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
