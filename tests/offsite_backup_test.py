import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
import shutil
import subprocess
from unittest import mock

spec = importlib.util.spec_from_file_location("receiver", Path(__file__).resolve().parents[1] / "scripts/hostinger_backup_receiver.py")
receiver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receiver)
sender_spec = importlib.util.spec_from_file_location("sender", Path(__file__).resolve().parents[1] / "scripts/offsite_backup.py")
sender = importlib.util.module_from_spec(sender_spec)
sender_spec.loader.exec_module(sender)


class ReceiverTests(unittest.TestCase):
    def test_valid_upload_replaces_only_after_verification(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            latest = root / "latest.dump.gpg"
            latest.write_bytes(b"previous verified backup")
            payload = b"new encrypted backup fixture"
            digest = hashlib.sha256(payload).hexdigest()
            result = receiver.receive(f"upload {digest} {len(payload)}", root, io.BytesIO(payload))
            self.assertEqual(latest.read_bytes(), payload)
            self.assertEqual(result, receiver.receive("status", root, io.BytesIO()))
            self.assertEqual(list(root.glob(".incoming-*")), [])

    def test_failed_upload_preserves_previous(self):
        for incoming, digest, size in [(b"partial", "0" * 64, 99), (b"corrupt", "0" * 64, 7)]:
            with self.subTest(incoming=incoming), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                latest = root / "latest.dump.gpg"
                latest.write_bytes(b"previous")
                with self.assertRaises(ValueError):
                    receiver.receive(f"upload {digest} {size}", root, io.BytesIO(incoming))
                self.assertEqual(latest.read_bytes(), b"previous")
                self.assertEqual(list(root.glob(".incoming-*")), [])

    def test_commands_and_size_are_restricted(self):
        with tempfile.TemporaryDirectory() as folder:
            for command in ["", "sh", "sftp", "rm latest.dump.gpg", "upload ../../bad 1", f"upload {'0'*64} 0", f"upload {'0'*64} {receiver.MAX_BYTES+1}"]:
                with self.subTest(command=command), self.assertRaises(ValueError):
                    receiver.receive(command, folder, io.BytesIO())


@unittest.skipUnless(shutil.which("gpg") and shutil.which("gpgconf"), "GnuPG required")
class EncryptionTests(unittest.TestCase):
    def test_encryption_roundtrip_and_remote_verification(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            destination = root / "remote"
            destination.mkdir()
            dump = root / "database.dump"
            dump.write_bytes(b"disposable fixture, never customer data\n" * 100)
            paths = {}
            for name, value in [("identity_file", "unused SSH fixture"), ("known_hosts_file", "unused host fixture"),
                                ("passphrase_file", "DisposableEncryptionFixture2026")]:
                path = root / name
                path.write_text(value)
                path.chmod(0o600)
                paths[name] = str(path)
            config = root / "config.json"
            config.write_text(json.dumps(dict(paths, enabled=True, host="fixture.example.invalid", user="fixture", port=65002)))
            run_original = subprocess.run

            def transport(args, **kwargs):
                if args[0] != "ssh":
                    return run_original(args, **kwargs)
                self.assertIn("StrictHostKeyChecking=yes", args)
                self.assertIn("BatchMode=yes", args)
                payload = kwargs["stdin"] if "stdin" in kwargs else io.BytesIO()
                response = receiver.receive(args[-1], destination, payload)
                return subprocess.CompletedProcess(args, 0, json.dumps(response).encode(), b"")

            with mock.patch.object(sender.subprocess, "run", side_effect=transport):
                result = sender.upload(dump, config)
            self.assertTrue(result["offsite_verified"])
            encrypted = destination / "latest.dump.gpg"
            self.assertNotIn(b"disposable fixture", encrypted.read_bytes())
            self.assertEqual(result["offsite_sha256"], receiver.digest_file(encrypted))


if __name__ == "__main__":
    unittest.main()
