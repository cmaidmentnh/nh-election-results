"""Signal feed - watches one group on the shared signal-cli daemon.

The daemon on 127.0.0.1:7583 already serves nh-whip-bot and signal-voter-bot;
each subscriber gets its own copy of the stream, so this adds a third without
disturbing them. Only the configured group is read - direct messages and every
other group are ignored.
"""

import json
import logging
import socket
import time
from pathlib import Path

from intake import config

log = logging.getLogger("intake.signal")

_counter = 0


def _rpc(sock, method, params, timeout=15):
    """One request/response round trip on its own short-lived connection."""
    global _counter
    _counter += 1
    rid = f"intake-{_counter}"
    sock.sendall((json.dumps({"jsonrpc": "2.0", "method": method,
                              "params": params, "id": rid}) + "\n").encode())
    sock.settimeout(timeout)
    buf = ""
    while True:
        data = sock.recv(65536)
        if not data:
            return None
        buf += data.decode("utf-8", errors="replace")
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("id") == rid:
                return msg


def resolve_group_id():
    """Find the watched group's id, by configured id or exact name."""
    if config.SIGNAL_GROUP_ID:
        return config.SIGNAL_GROUP_ID
    if not config.SIGNAL_GROUP_NAME:
        return None
    with socket.create_connection((config.SIGNAL_HOST, config.SIGNAL_PORT), timeout=15) as s:
        resp = _rpc(s, "listGroups", {"account": config.SIGNAL_ACCOUNT})
    for g in (resp or {}).get("result", []) or []:
        if (g.get("name") or "").strip().lower() == config.SIGNAL_GROUP_NAME.strip().lower():
            log.info("Watching Signal group %r (%s)", g.get("name"), g.get("id"))
            return g.get("id")
    log.error("No Signal group named %r - is the bot a member yet?", config.SIGNAL_GROUP_NAME)
    return None


def _attachment_paths(data_msg):
    paths = []
    for att in data_msg.get("attachments") or []:
        aid = att.get("id")
        if not aid:
            continue
        p = Path(config.SIGNAL_ATTACH_DIR) / aid
        if p.exists():
            paths.append(str(p))
    return paths


def listen(on_message, stop=None):
    """Block forever, calling on_message(dict) for each group message."""
    group_id = resolve_group_id()

    while not (stop and stop.is_set()):
        try:
            log.info("Connecting to signal-cli %s:%s", config.SIGNAL_HOST, config.SIGNAL_PORT)
            sock = socket.create_connection((config.SIGNAL_HOST, config.SIGNAL_PORT), timeout=15)
            sock.settimeout(None)
            sock.sendall((json.dumps({
                "jsonrpc": "2.0", "method": "subscribeReceive",
                "params": {"account": config.SIGNAL_ACCOUNT}, "id": "intake-sub",
            }) + "\n").encode())

            if not group_id:
                group_id = resolve_group_id()

            buf = ""
            while not (stop and stop.is_set()):
                data = sock.recv(65536)
                if not data:
                    log.warning("signal-cli closed the connection")
                    break
                buf += data.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if msg.get("method") != "receive":
                        continue

                    envelope = (msg.get("params") or {}).get("envelope") or {}
                    data_msg = envelope.get("dataMessage") or {}
                    ginfo = data_msg.get("groupInfo") or {}

                    if not group_id or ginfo.get("groupId") != group_id:
                        continue  # not the results group

                    body = data_msg.get("message") or ""
                    attachments = _attachment_paths(data_msg)
                    if not body.strip() and not attachments:
                        continue

                    sender = (envelope.get("sourceName")
                              or envelope.get("sourceNumber")
                              or envelope.get("sourceUuid") or "unknown")
                    ts = data_msg.get("timestamp") or envelope.get("timestamp")
                    on_message({
                        "source": "signal",
                        "external_id": f"{envelope.get('sourceUuid') or sender}:{ts}",
                        "sender": sender,
                        "subject": "",
                        "body": body,
                        "attachments": attachments,
                        "received_at": None,
                    })
        except Exception:
            log.exception("Signal listener error; reconnecting in 10s")
        finally:
            try:
                sock.close()
            except Exception:
                pass
        if not (stop and stop.is_set()):
            time.sleep(10)


def send(text, target=None):
    """Send a short operator notification."""
    target = target or config.NOTIFY_TARGET
    if not target or not config.SIGNAL_ACCOUNT:
        return
    params = {"account": config.SIGNAL_ACCOUNT, "message": text}
    # Group ids are long base64; phone numbers start with '+'.
    if target.startswith("+"):
        params["recipient"] = [target]
    else:
        params["groupId"] = target
    try:
        with socket.create_connection((config.SIGNAL_HOST, config.SIGNAL_PORT), timeout=15) as s:
            _rpc(s, "send", params)
    except Exception:
        log.exception("Could not send Signal notification")
