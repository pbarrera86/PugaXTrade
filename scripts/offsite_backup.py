#!/usr/bin/env python3
"""Encrypt a verified dump with GnuPG, then atomically upload via restricted SSH."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def digest_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def upload(dump, config_path):
    config = json.loads(Path(config_path).read_text())
    if not config.get("enabled"):
        raise ValueError("Offsite backup is not enabled")
    host, user, port = config["host"], config["user"], int(config["port"])
    if not re.fullmatch(r"[A-Za-z0-9.-]+", host) or not re.fullmatch(r"[A-Za-z0-9_-]+", user) or not 1 <= port <= 65535:
        raise ValueError("Invalid SSH configuration")
    for name in ("identity_file", "passphrase_file", "known_hosts_file"):
        path = Path(config[name])
        if not path.is_file() or path.stat().st_mode & 0o077:
            raise ValueError("Private configuration file permissions required")
    ssh = ["ssh", "-F", "/dev/null", "-i", config["identity_file"], "-p", str(port),
           "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
           "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no",
           "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + config["known_hosts_file"],
           "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2", user + "@" + host]
    with tempfile.TemporaryDirectory(prefix=".offsite-", dir=Path(dump).parent) as work:
        home = Path(work) / "gpg"
        home.mkdir(mode=0o700)
        encrypted = Path(work) / "dump.gpg"
        gpg = ["gpg", "--homedir", str(home), "--batch", "--yes", "--pinentry-mode", "loopback",
               "--no-symkey-cache", "--passphrase-file", config["passphrase_file"]]
        try:
            subprocess.run(gpg + ["--cipher-algo", "AES256", "--s2k-mode", "3", "--s2k-digest-algo", "SHA256",
                                 "--s2k-count", "65011712", "--force-mdc", "--output", str(encrypted), "--symmetric", str(dump)],
                           check=True, timeout=600, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # Decrypt and hash without writing a second plaintext dump or holding it in RAM.
            h = hashlib.sha256()
            with subprocess.Popen(gpg + ["--decrypt", str(encrypted)], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as process:
                for block in iter(lambda: process.stdout.read(1024 * 1024), b""):
                    h.update(block)
                if process.wait(timeout=600) or h.hexdigest() != digest_file(dump):
                    raise ValueError("Encrypted backup verification failed")
            expected = {"sha256": digest_file(encrypted), "bytes": encrypted.stat().st_size}
            with open(encrypted, "rb") as payload:
                response = subprocess.run(ssh + ["upload " + expected["sha256"] + " " + str(expected["bytes"])],
                                          stdin=payload, capture_output=True, check=True, timeout=600)
            if json.loads(response.stdout) != expected:
                raise ValueError("Remote upload verification failed")
            status = subprocess.run(ssh + ["status"], capture_output=True, check=True, timeout=60)
            if json.loads(status.stdout) != expected:
                raise ValueError("Remote stored-file verification failed")
            return {"offsite_verified": True, "offsite_sha256": expected["sha256"], "offsite_bytes": expected["bytes"]}
        finally:
            subprocess.run(["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("dump", type=Path)
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    print(json.dumps(upload(args.dump, args.config)))
