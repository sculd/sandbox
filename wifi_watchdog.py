#!/usr/bin/env python3
"""Wifi watchdog: check internet connectivity and bounce wifi if it is down.

Usage:
  wifi_watchdog.py                 one check; bounce wifi (radio off/on) if the
                                   internet is unreachable  (what the timer runs)
  wifi_watchdog.py --dry-run       one check; report only, never toggle wifi
  wifi_watchdog.py --loop 1800     keep running, check every N seconds
  wifi_watchdog.py --force-bounce  bounce wifi now even if the internet is up
                                   (end-to-end test of the recovery path)

Everything is logged to stdout (journalctl --user -u wifi-watchdog).
Outages and recoveries are also appended to ~/.local/state/wifi-watchdog/events.log
so you can see how often this actually happens.

Only the standard library is used; run it with /usr/bin/python3.
"""

import argparse
import datetime
import pathlib
import socket
import subprocess
import sys
import time

# Raw-IP probes: no DNS needed. All must fail before we call the internet "down".
TCP_PROBES = [("1.1.1.1", 443), ("8.8.8.8", 443), ("9.9.9.9", 443)]
# Name probes: catch the "IP works but DNS is broken" case. All must fail.
DNS_PROBES = ["www.google.com", "one.one.one.one"]
PROBE_TIMEOUT_S = 5

RADIO_OFF_S = 5            # how long to leave the radio off before turning it on
RECOVERY_WAIT_S = 90       # how long to wait for the internet to come back after a bounce
RECOVERY_POLL_S = 5

EVENT_LOG = pathlib.Path.home() / ".local" / "state" / "wifi-watchdog" / "events.log"


def _ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"{_ts()} {msg}", flush=True)


def event(msg: str) -> None:
    """Log and also append to the persistent events file."""
    log(msg)
    try:
        EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with EVENT_LOG.open("a") as f:
            f.write(f"{_ts()} {msg}\n")
    except OSError as e:
        log(f"warning: could not write {EVENT_LOG}: {e}")


def tcp_reachable() -> bool:
    for host, port in TCP_PROBES:
        try:
            with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_S):
                return True
        except OSError:
            continue
    return False


def dns_works() -> bool:
    for name in DNS_PROBES:
        try:
            socket.getaddrinfo(name, 443)
            return True
        except OSError:
            continue
    return False


def internet_status() -> str:
    """Return 'up', 'dns-broken' (IPs reachable but no name resolution) or 'down'."""
    if not tcp_reachable():
        return "down"
    return "up" if dns_works() else "dns-broken"


def nmcli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["nmcli", *args], capture_output=True, text=True, timeout=30)


def wifi_radio_enabled() -> bool:
    return nmcli("radio", "wifi").stdout.strip() == "enabled"


def bounce_wifi() -> bool:
    """Turn the wifi radio off then on; return True once the internet is back."""
    r = nmcli("radio", "wifi", "off")
    if r.returncode != 0:
        event(f"nmcli radio wifi off failed: {r.stderr.strip()}")
        return False
    time.sleep(RADIO_OFF_S)
    r = nmcli("radio", "wifi", "on")
    if r.returncode != 0:
        event(f"nmcli radio wifi on failed: {r.stderr.strip()}")
        return False

    deadline = time.monotonic() + RECOVERY_WAIT_S
    while time.monotonic() < deadline:
        time.sleep(RECOVERY_POLL_S)
        if internet_status() == "up":
            return True
    return False


def check_once(dry_run: bool = False, force_bounce: bool = False) -> int:
    status = internet_status()
    if status == "up" and not force_bounce:
        log("internet up")
        return 0

    if status != "up":
        event(f"internet {status}")
    else:
        log("internet up, but --force-bounce given")

    if not wifi_radio_enabled():
        log("wifi radio is switched off; assuming that is deliberate, not touching it")
        return 0

    if dry_run:
        log("dry-run: would bounce wifi (radio off, wait, radio on)")
        return 0

    event("bouncing wifi: radio off/on")
    started = time.monotonic()
    if bounce_wifi():
        event(f"internet restored {time.monotonic() - started:.0f}s after wifi bounce")
        return 0
    event(f"internet still not up {RECOVERY_WAIT_S}s after wifi bounce; will retry next run")
    return 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="report only, never toggle wifi")
    p.add_argument("--force-bounce", action="store_true", help="bounce wifi even if the internet is up")
    p.add_argument("--loop", type=int, metavar="SECONDS", help="keep running, checking every SECONDS")
    args = p.parse_args()

    if args.loop is None:
        return check_once(args.dry_run, args.force_bounce)

    log(f"looping every {args.loop}s")
    while True:
        try:
            check_once(args.dry_run, args.force_bounce)
        except Exception as e:  # keep the loop alive no matter what
            log(f"error during check: {e!r}")
        time.sleep(args.loop)


if __name__ == "__main__":
    sys.exit(main())
