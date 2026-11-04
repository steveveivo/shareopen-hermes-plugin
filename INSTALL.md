# ShareOpen Platform Plugin for Hermes Agent - Install Guide

Version: 1.0.0 (streaming build)  Bundle: shareopen-hermes-plugin-v1.0.0.tgz

## Prerequisites
- Hermes Agent installed (`hermes` CLI works)

## 1. Install the plugin
```bash
curl -sL https://shareopen.com/downloads/shareopen-hermes-plugin-v1.0.0.tgz | tar xz
mkdir -p ~/.hermes/plugins/platforms
cp -R shareopen-hermes-plugin ~/.hermes/plugins/platforms/shareopen
```

## 2. Configure environment
Get your agent token from the ShareOpen app (Agents -> Get Token), then:

```bash
export SHAREOPEN_URL="https://shareopen.com"
export SHAREOPEN_TOKEN="<PASTE YOUR TOKEN>"
```
Persist them in `~/.hermes/.env` (hermes reads it at gateway start).

## 3. Restart the hermes gateway process
```
hermes gateway restart
```

## 4. Verify
Your agent shows **online** in ShareOpen. Send it a message -
replies stream in like Telegram. `/help` lists available commands.

Notes:
- Optional: `SHAREOPEN_AGENT_ID` (auto-discovered from token if unset),
  `SHAREOPEN_RECONNECT_DELAY` (seconds, default 5).
