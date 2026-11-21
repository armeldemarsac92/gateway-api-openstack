#!/usr/bin/env python3
"""Exercise stock Amphora LVS configurations in a prepared disposable guest."""

import argparse
from contextlib import ExitStack, contextmanager, suppress
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time


VIP = ("192.0.2.30", 18053)
BACKENDS = (("198.51.100.11", 15001, "A", 3), ("198.51.100.12", 15002, "B", 1))
PAYLOAD = bytes(range(256))


def echo(address, port, identity):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
        server.bind((address, int(port)))
        print("ready", flush=True)
        while True:
            packet, peer = server.recvfrom(512)
            print(f"received {len(packet)} bytes", flush=True)
            server.sendto(identity.encode() + packet, peer)


def query(address, port, identity):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
        client.settimeout(1)
        client.connect((address, int(port)))
        client.send(PAYLOAD)
        try:
            reply = client.recv(512)
        except (ConnectionRefusedError, TimeoutError) as error:
            if identity != "detached":
                raise
            outcome = "ECONNREFUSED" if isinstance(error, ConnectionRefusedError) else "timeout"
            print(f"{address}:{port} returned no UDP echo ({outcome})", flush=True)
            return
        if identity == "detached":
            raise RuntimeError(f"Detached listener returned a UDP reply of {len(reply)} bytes")
        if reply != identity.encode() + PAYLOAD:
            raise RuntimeError(f"Expected backend {identity} and the unchanged UDP payload")
        print(f"{address}:{port} echoed all {len(PAYLOAD)} bytes from backend {identity}",
              flush=True)


def socket_command(namespace, mode, address, port, identity):
    return (
        "ip", "netns", "exec", namespace, sys.executable, "-S",
        str(Path(__file__).resolve()), mode, address, str(port), identity,
    )


@contextmanager
def running(command, **kwargs):
    with subprocess.Popen(command, start_new_session=True, **kwargs) as process:
        try:
            yield process
        finally:
            try:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=5)
            finally:
                # Stop any checker child if Keepalived failed to shut down cleanly.
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)


def wait_for(process, read, ready, description):
    deadline = time.monotonic() + 5
    while True:
        if process.poll() is not None:
            raise RuntimeError(f"{description} exited with status {process.returncode}")
        current = read()
        if ready(current):
            return current
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for {description}\n{current}")
        time.sleep(0.05)


def ipvs_address(address, port):
    return f"{socket.inet_aton(address).hex().upper()}:{port:04X}"


def ipvs_table():
    return subprocess.run(
        ["ip", "netns", "exec", "amphora-haproxy", "cat", "/proc/net/ip_vs"],
        check=True, capture_output=True, text=True, timeout=3,
    ).stdout


def matches_ipvs(table, backend):
    rows = [line.split() for line in table.splitlines()[3:] if line.strip()]
    if backend is None:
        return not rows
    address, port, _, weight = backend
    return (
        len(rows) == 2
        and rows[0][:3] == ["UDP", ipvs_address(*VIP), "wrr"]
        and rows[1][:4] == ["->", ipvs_address(address, port), "Masq", str(weight)]
    )


def traffic(config_dir, keepalived):
    with tempfile.TemporaryDirectory(prefix="stock-udp-") as directory, ExitStack() as stack:
        logs = []
        for address, port, identity, _ in BACKENDS:
            log = Path(directory) / f"backend-{identity}.log"
            with log.open("w", encoding="utf-8") as output:
                process = stack.enter_context(running(
                    socket_command("backend", "echo", address, port, identity), stdout=output,
                ))
            wait_for(process, log.read_text, lambda text: text.startswith("ready\n"),
                     f"backend {identity}")
            logs.append(log)

        for state, backend in zip(("pool-a", "pool-b", "detached"), (*BACKENDS, None), strict=True):
            config = config_dir / f"{state}.conf"
            if backend is None and "virtual_server" in config.read_text(encoding="utf-8"):
                raise RuntimeError(f"Detached configuration contains a virtual server: {config}")
            subprocess.run([keepalived, "--config-test", "-f", config], check=True, timeout=5)
            command = [keepalived, "--dont-fork", "--log-console", "--check", "-f", config]
            with running(command) as process:
                services = wait_for(
                    process, ipvs_table, lambda table: matches_ipvs(table, backend), state,
                )
                print(f"IPVS for {state}\n{services}", flush=True)
                identity = backend[2] if backend else "detached"
                subprocess.run(
                    socket_command("client", "query", *VIP, identity), check=True, timeout=5,
                )

        for address, port, identity, _ in BACKENDS:
            subprocess.run(
                socket_command("client", "query", address, port, identity), check=True, timeout=5,
            )
        for log, (_, _, identity, _) in zip(logs, BACKENDS, strict=True):
            packets = log.read_text(encoding="utf-8").splitlines()[1:]
            if packets != ["received 256 bytes", "received 256 bytes"]:
                raise RuntimeError(f"Backend {identity} expected exactly two packets, got {packets}")
            print(f"Backend {identity} observed one VIP packet and one direct control packet",
                  flush=True)

    print("STOCK_UDP_TRAFFIC_PASS", flush=True)


def exit_on_term(signum, _frame):
    raise SystemExit(128 + signum)


def main():
    if sys.argv[1:2] == ["echo"]:
        echo(*sys.argv[2:])
        return
    if sys.argv[1:2] == ["query"]:
        query(*sys.argv[2:])
        return

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config_dir", type=Path, help="Directory emitted by stock-udp.py")
    parser.add_argument("--keepalived", default="keepalived", help="Keepalived binary")
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, exit_on_term)
    traffic(args.config_dir.resolve(), args.keepalived)


if __name__ == "__main__":
    main()
