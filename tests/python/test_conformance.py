#!/usr/bin/env python3
"""
Dragonfly-Style Python Conformance & Client Integration Test Suite for Rudis.
Compatible with standard library `unittest` and `pytest`.
Communicates directly via TCP sockets with full RESP2/RESP3 decoding.
"""

import os
import socket
import unittest

DEFAULT_HOST = os.environ.get("RUDIS_TEST_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("RUDIS_TEST_PORT", "16420"))


class RespClient:
    """Minimal, robust zero-dependency RESP client for testing."""

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.reader = self.sock.makefile("rb")

    def execute(self, *args):
        req = self._encode_cmd(args)
        self.sock.sendall(req)
        return self._read_response()

    def pipeline(self, commands):
        payload = bytearray()
        for cmd in commands:
            payload.extend(self._encode_cmd(cmd))
        self.sock.sendall(payload)
        return [self._read_response() for _ in commands]

    def close(self):
        try:
            self.reader.close()
            self.sock.close()
        except Exception:
            pass

    def _encode_cmd(self, args):
        out = bytearray(f"*{len(args)}\r\n".encode("utf-8"))
        for arg in args:
            if isinstance(arg, bytes):
                data = arg
            else:
                data = str(arg).encode("utf-8")
            out.extend(f"${len(data)}\r\n".encode("utf-8"))
            out.extend(data)
            out.extend(b"\r\n")
        return bytes(out)

    def _read_response(self):
        prefix = self.reader.read(1)
        if not prefix:
            raise ConnectionError("Server closed connection")

        if prefix == b"+":
            return self.reader.readline().rstrip(b"\r\n").decode("utf-8")
        elif prefix == b"-":
            err = self.reader.readline().rstrip(b"\r\n").decode("utf-8")
            return Exception(err)
        elif prefix == b":":
            return int(self.reader.readline().rstrip(b"\r\n"))
        elif prefix == b"$":
            length = int(self.reader.readline().rstrip(b"\r\n"))
            if length == -1:
                return None
            data = self.reader.read(length)
            self.reader.read(2)  # Discard \r\n
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data
        elif prefix == b"*":
            count = int(self.reader.readline().rstrip(b"\r\n"))
            if count == -1:
                return None
            return [self._read_response() for _ in range(count)]
        elif prefix == b"%":
            count = int(self.reader.readline().rstrip(b"\r\n"))
            res = {}
            for _ in range(count):
                k = self._read_response()
                v = self._read_response()
                res[k] = v
            return res
        elif prefix == b"_":
            self.reader.readline()
            return None
        elif prefix == b",":
            return float(self.reader.readline().rstrip(b"\r\n"))
        else:
            raise ValueError(f"Unknown RESP prefix: {prefix}")


class RudisPythonConformanceTest(unittest.TestCase):
    def setUp(self):
        self.client = RespClient()

    def tearDown(self):
        self.client.close()

    def test_strings_and_keyspace_crud(self):
        # SET / GET
        res = self.client.execute("SET", "{conf}:k1", "val1")
        self.assertEqual(res, "OK")
        self.assertEqual(self.client.execute("GET", "{conf}:k1"), "val1")

        # INCR / DECR
        self.client.execute("SET", "{conf}:num", "10")
        self.assertEqual(self.client.execute("INCR", "{conf}:num"), 11)
        self.assertEqual(self.client.execute("INCRBY", "{conf}:num", "5"), 16)
        self.assertEqual(self.client.execute("DECR", "{conf}:num"), 15)

        # MSET / MGET co-located
        self.client.execute("MSET", "{conf}:a", "1", "{conf}:b", "2")
        self.assertEqual(self.client.execute("MGET", "{conf}:a", "{conf}:b"), ["1", "2"])

        # EXISTS / DEL
        self.assertEqual(self.client.execute("EXISTS", "{conf}:k1", "{conf}:nonexistent"), 1)
        self.assertEqual(self.client.execute("DEL", "{conf}:k1", "{conf}:num"), 2)
        self.assertIsNone(self.client.execute("GET", "{conf}:k1"))

    def test_hashes_and_lists(self):
        # Hashes
        self.assertEqual(self.client.execute("HSET", "{h}:user1", "name", "Alice", "age", "30"), 2)
        self.assertEqual(self.client.execute("HGET", "{h}:user1", "name"), "Alice")
        hvals = self.client.execute("HGETALL", "{h}:user1")
        self.assertIn("name", hvals)
        self.assertIn("Alice", hvals)

        # Lists
        self.assertEqual(self.client.execute("RPUSH", "{l}:list1", "a", "b", "c"), 3)
        self.assertEqual(self.client.execute("LLEN", "{l}:list1"), 3)
        self.assertEqual(self.client.execute("LPOP", "{l}:list1"), "a")
        self.assertEqual(self.client.execute("RPOP", "{l}:list1"), "c")
        self.assertEqual(self.client.execute("LRANGE", "{l}:list1", "0", "-1"), ["b"])

    def test_pipelining_burst(self):
        count = 200
        cmds = [("SET", f"{{pipe}}:k{i}", f"v{i}") for i in range(count)]
        responses = self.client.pipeline(cmds)
        self.assertEqual(len(responses), count)
        for r in responses:
            self.assertEqual(r, "OK")

        # Verify values in batch
        get_cmds = [("GET", f"{{pipe}}:k{i}") for i in range(count)]
        get_responses = self.client.pipeline(get_cmds)
        self.assertEqual(len(get_responses), count)
        for i, val in enumerate(get_responses):
            self.assertEqual(val, f"v{i}")

    def test_multi_exec_atomic(self):
        self.client.execute("SET", "{txn}:acc", "100")
        self.assertEqual(self.client.execute("MULTI"), "OK")
        self.assertEqual(self.client.execute("INCRBY", "{txn}:acc", "50"), "QUEUED")
        self.assertEqual(self.client.execute("GET", "{txn}:acc"), "QUEUED")
        res = self.client.execute("EXEC")
        self.assertEqual(res, [150, "150"])

    def test_json_and_vectors(self):
        # RedisJSON
        self.assertEqual(
            self.client.execute("JSON.SET", "{doc}:1", "$", '{"title":"Rudis","stars":5}'),
            "OK",
        )
        doc = self.client.execute("JSON.GET", "{doc}:1", "$")
        self.assertIn("Rudis", doc)

        # Vector HNSW operations
        vadd_res = self.client.execute("VADD", "{v}:idx", "VALUES", "2", "1.0", "0.0", "doc_a")
        self.assertIn(vadd_res, [1, "1", "OK", 0])
        sim_res = self.client.execute("VQUERY", "{v}:idx", "1", "1.0", "0.0")
        self.assertTrue(len(sim_res) >= 1)

    def test_error_handling_parity(self):
        # Wrong argument counts
        err = self.client.execute("GET")
        self.assertIsInstance(err, Exception)
        self.assertTrue("wrong number of arguments" in str(err).lower())

        # Wrong type
        self.client.execute("SET", "{type}:str", "hello")
        err_type = self.client.execute("LPUSH", "{type}:str", "world")
        self.assertIsInstance(err_type, Exception)
        self.assertTrue("WRONGTYPE" in str(err_type))


if __name__ == "__main__":
    unittest.main()
