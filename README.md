# sandbox

## wifi_watchdog.py

Checks the internet connection and, if it is down, bounces wifi
(`nmcli radio wifi off` / `on`), which is what fixes the occasional dropout on
this machine. Runs as a systemd user timer every 10 minutes (also 3 minutes
after login). Standard library only; run with `/usr/bin/python3`.

Units live outside the repo:

- `~/.config/systemd/user/wifi-watchdog.service`
- `~/.config/systemd/user/wifi-watchdog.timer`

### Commands

```bash
# one check, report only, never toggles wifi
/usr/bin/python3 ~/projects/sandbox/wifi_watchdog.py --dry-run

# end-to-end test: bounces wifi even if the internet is up (drops wifi ~10-20s)
/usr/bin/python3 ~/projects/sandbox/wifi_watchdog.py --force-bounce

# run it in the foreground instead of via systemd, checking every N seconds
/usr/bin/python3 ~/projects/sandbox/wifi_watchdog.py --loop 600

# logs
journalctl --user -u wifi-watchdog.service -n 30     # every run
cat ~/.local/state/wifi-watchdog/events.log         # outages and recoveries only

# timer
systemctl --user list-timers wifi-watchdog.timer    # next scheduled run
systemctl --user start wifi-watchdog.service        # run a check right now
systemctl --user stop wifi-watchdog.timer           # pause the watchdog
systemctl --user start wifi-watchdog.timer          # resume it
```

### Changing the interval

Edit `OnUnitActiveSec` in `~/.config/systemd/user/wifi-watchdog.timer`, then:

```bash
systemctl --user daemon-reload
systemctl --user restart wifi-watchdog.timer
```
