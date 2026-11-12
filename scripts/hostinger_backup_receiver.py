#!/usr/bin/env python3
"""Forced-command SSH receiver: one encrypted backup, no shell or file browsing."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

MAX_BYTES = 1024 ** 3


def digest_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def receive(command, directory, stream):
    directory = Path(directory)
    latest = directory / "latest.dump.gpg"
    parts = command.split()
    if parts == ["status"]:
        if not latest.is_file() or latest.is_symlink():
            raise ValueError("No verified backup")
        return {"sha256": digest_file(latest), "bytes": latest.stat().st_size}
    if len(parts) != 3 or parts[0] != "upload":
        raise ValueError("Command not permitted")
    digest, size_text = parts[1:]
    if not re.fullmatch(r"[a-f0-9]{64}", digest) or not re.fullmatch(r"[0-9]{1,11}", size_text):
        raise ValueError("Invalid manifest")
    size = int(size_text)
    if not 1 <= size <= MAX_BYTES:
        raise ValueError("Backup size exceeds limit")
    if shutil.disk_usage(directory).free < size + 10 * 1024 ** 2:
        raise ValueError("Insufficient space")
    with open(directory / ".receiver.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fd, temporary = tempfile.mkstemp(prefix=".incoming-", dir=directory)
        try:
            h = hashlib.sha256()
            remaining = size
            with os.fdopen(fd, "wb") as target:
                while remaining:
                    block = stream.read(min(1024 * 1024, remaining))
                    if not block:
                        raise ValueError("Incomplete transfer")
                    target.write(block)
                    h.update(block)
                    remaining -= len(block)
                target.flush()
                os.fsync(target.fileno())
            if h.hexdigest() != digest:
                raise ValueError("Checksum mismatch")
            # The previous file survives every failure before this atomic rename.
            os.replace(temporary, latest)
            folder_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(folder_fd)
            finally:
                os.close(folder_fd)
            return {"sha256": digest, "bytes": size}
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


if __name__ == "__main__":
    os.umask(0o077)
    try:
        result = receive(os.environ.get("SSH_ORIGINAL_COMMAND", ""), Path(__file__).resolve().parent, sys.stdin.buffer)
        print(json.dumps(result))
    except Exception as error:
        print("Backup transfer rejected: " + type(error).__name__, file=sys.stderr)
        sys.exit(1)
