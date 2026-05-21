import copy
import binascii
import os
import socket
import threading
import time
import uuid
from typing import Any, Dict, Optional


class TxContext:
    def __init__(self, tx_id: str, snapshot: Dict[str, str]):
        self.tx_id = tx_id
        self.snapshot = snapshot
        self.writes: Dict[str, str] = {}
        self.active = True


class MiniTreeBranch:
    def __init__(self, branch_id: int, parent_branch_id: int):
        self.branch_id = branch_id
        self.parent_branch_id = parent_branch_id
        self.writes: Dict[str, str] = {}


class MiniTreeContext:
    def __init__(self, tx_id: str, snapshot: Dict[str, str]):
        self.tx_id = tx_id
        self.snapshot = snapshot
        self.branches: Dict[int, MiniTreeBranch] = {}
        self.next_branch_id = 1
        self.winner_branch_id = None
        self.active = True
        self.last_abort_reason = ""


class MiniDBClient:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._store: Dict[str, str] = {}
        self._tx_map: Dict[str, TxContext] = {}
        self._tree_map: Dict[str, MiniTreeContext] = {}

    def kv_get(self, key: str) -> Optional[str]:
        with self._lock:
            return self._store.get(key)

    def start(self) -> str:
        with self._lock:
            tx_id = str(uuid.uuid4())
            self._tx_map[tx_id] = TxContext(tx_id=tx_id, snapshot=copy.deepcopy(self._store))
            return tx_id

    def _require_tx(self, tx_id: str) -> TxContext:
        with self._lock:
            tx = self._tx_map.get(tx_id)
            if tx is None or not tx.active:
                raise RuntimeError("invalid or inactive tx_id: {}".format(tx_id))
            return tx

    def get(self, tx_id: str, key: str) -> Optional[str]:
        with self._lock:
            tx = self._require_tx(tx_id)
            if key in tx.writes:
                return tx.writes[key]
            return tx.snapshot.get(key)

    def put(self, tx_id: str, key: str, value: str) -> None:
        with self._lock:
            tx = self._require_tx(tx_id)
            tx.writes[key] = value

    def commit(self, tx_id: str) -> None:
        with self._lock:
            tx = self._require_tx(tx_id)
            self._store.update(tx.writes)
            tx.active = False
            del self._tx_map[tx_id]

    def rollback(self, tx_id: str) -> None:
        with self._lock:
            tx = self._require_tx(tx_id)
            tx.active = False
            del self._tx_map[tx_id]

    def tstart(self) -> str:
        with self._lock:
            tx_id = str(uuid.uuid4())
            ctx = MiniTreeContext(tx_id=tx_id, snapshot=copy.deepcopy(self._store))
            ctx.branches[0] = MiniTreeBranch(branch_id=0, parent_branch_id=0)
            self._tree_map[tx_id] = ctx
            return tx_id

    def _require_tree_tx(self, tx_id: str) -> MiniTreeContext:
        with self._lock:
            ctx = self._tree_map.get(tx_id)
            if ctx is None or not ctx.active:
                raise RuntimeError("invalid or inactive tree tx_id: {}".format(tx_id))
            return ctx

    def tbranch(self, tx_id: str, parent_branch_id: int) -> int:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            if parent_branch_id not in ctx.branches:
                raise RuntimeError("unknown parent branch_id: {}".format(parent_branch_id))
            branch_id = ctx.next_branch_id
            ctx.next_branch_id += 1
            ctx.branches[branch_id] = MiniTreeBranch(branch_id=branch_id, parent_branch_id=parent_branch_id)
            return branch_id

    def tbranch_many(self, tx_id: str, parent_branch_id: int, count: int):
        return [self.tbranch(tx_id, parent_branch_id) for _ in range(max(0, count))]

    def _resolve_branch_value(self, ctx: MiniTreeContext, branch_id: int, key: str) -> Optional[str]:
        current = ctx.branches.get(branch_id)
        while current is not None:
            if key in current.writes:
                return current.writes[key]
            if current.parent_branch_id == current.branch_id:
                break
            current = ctx.branches.get(current.parent_branch_id)
        return ctx.snapshot.get(key)

    def tget(self, tx_id: str, branch_id: int, key: str, strict: bool = False) -> Optional[str]:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            if branch_id not in ctx.branches:
                raise RuntimeError("unknown branch_id: {}".format(branch_id))
            if strict:
                return ctx.snapshot.get(key)
            return self._resolve_branch_value(ctx, branch_id, key)

    def tget_many(self, tx_id: str, requests, strict: bool = False):
        return [self.tget(tx_id, branch_id, key, strict=strict) for branch_id, key in requests]

    def tput(self, tx_id: str, branch_id: int, key: str, value: str) -> None:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            branch = ctx.branches.get(branch_id)
            if branch is None:
                raise RuntimeError("unknown branch_id: {}".format(branch_id))
            if branch_id == 0:
                raise RuntimeError("root branch is read-only in mini tree mode")
            branch.writes[key] = value

    def tput_many(self, tx_id: str, requests) -> None:
        for branch_id, key, value in requests:
            self.tput(tx_id, branch_id, key, value)

    def twinner(self, tx_id: str, branch_id: int) -> None:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            if branch_id not in ctx.branches:
                raise RuntimeError("unknown winner branch_id: {}".format(branch_id))
            ctx.winner_branch_id = branch_id

    def trefresh_winner(self, tx_id: str, branch_id: int, key: str) -> Optional[str]:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            if ctx.winner_branch_id != branch_id:
                raise RuntimeError("branch {} is not the winner".format(branch_id))
            return ctx.snapshot.get(key)

    def tcommit(self, tx_id: str) -> None:
        self.tcommit_retry(tx_id)

    def tcommit_retry(self, tx_id: str) -> None:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            if ctx.winner_branch_id is None:
                raise RuntimeError("winner branch is not selected")
            winner = ctx.branches.get(ctx.winner_branch_id)
            if winner is None:
                raise RuntimeError("winner branch is missing")

            merged: Dict[str, str] = {}
            lineage = []
            current = winner
            while current is not None:
                lineage.append(current)
                if current.parent_branch_id == current.branch_id:
                    break
                current = ctx.branches.get(current.parent_branch_id)
            for branch in reversed(lineage):
                merged.update(branch.writes)

            self._store.update(merged)
            ctx.active = False
            del self._tree_map[tx_id]

    def tabort(self, tx_id: str) -> None:
        with self._lock:
            ctx = self._require_tree_tx(tx_id)
            ctx.active = False
            del self._tree_map[tx_id]

    def dump_all(self) -> Dict[str, Any]:
        with self._lock:
            return {"ok": True, "data": dict(self._store), "size": len(self._store)}


mini_db = MiniDBClient()


class TreeDBSession:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.buf = b""

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=3.0)
        self.sock.settimeout(5.0)
        self._readline()

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            self.buf = b""

    def _readline(self) -> str:
        if self.sock is None:
            raise RuntimeError("Tree-DB session is not connected")
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line.decode("utf-8", errors="replace").strip()

    def request(self, cmd: str) -> str:
        if self.sock is None:
            raise RuntimeError("Tree-DB session is not connected")
        self.sock.sendall((cmd + "\n").encode("utf-8"))
        return self._readline()


class AgentDBClient:
    def __init__(self):
        self.backend = os.getenv("TREE_DB_BACKEND", "socket").strip().lower()
        self.host = os.getenv("TREE_DB_HOST", "127.0.0.1")
        self.port = int(os.getenv("TREE_DB_PORT", "19091"))
        self.allow_fallback = os.getenv("TREE_DB_FALLBACK_TO_MINI", "0").strip().lower() in {"1", "true", "yes"}
        self._mini = mini_db
        self._sessions: Dict[str, TreeDBSession] = {}
        self._tree_sessions: Dict[str, TreeDBSession] = {}

    def _using_mini(self) -> bool:
        return self.backend == "mini"

    def _open_session(self) -> TreeDBSession:
        sess = TreeDBSession(self.host, self.port)
        sess.connect()
        return sess

    @staticmethod
    def _is_ok(resp: str) -> bool:
        upper = resp.upper()
        return upper.startswith("OK") or upper.startswith("VALUE") or upper.startswith("FOUND")

    @staticmethod
    def _is_not_found(resp: str) -> bool:
        return "NOT_FOUND" in resp.upper()

    @staticmethod
    def _is_abort(resp: str) -> bool:
        return "ABORT" in resp.upper()

    @staticmethod
    def _extract_value_payload(resp: str) -> Optional[str]:
        upper = resp.upper()
        if upper.startswith("OK VALUE "):
            return resp[9:]
        if upper.startswith("VALUE "):
            return resp[6:]
        return None

    @staticmethod
    def _hex_encode(value: str) -> str:
        return binascii.hexlify(value.encode("utf-8")).decode("ascii")

    @staticmethod
    def _hex_decode(value: str) -> str:
        return binascii.unhexlify(value.encode("ascii")).decode("utf-8")

    def _require_session(self, tx_id: str) -> TreeDBSession:
        session = self._sessions.get(tx_id)
        if session is None:
            raise RuntimeError("invalid or inactive tx_id: {}".format(tx_id))
        return session

    def _require_tree_session(self, tx_id: str) -> TreeDBSession:
        session = self._tree_sessions.get(tx_id)
        if session is None:
            raise RuntimeError("invalid or inactive tree tx_id: {}".format(tx_id))
        return session

    def kv_get(self, key: str) -> Optional[str]:
        if self._using_mini():
            return self._mini.kv_get(key)
        try:
            last_exc = None
            for attempt in range(10):
                session = self._open_session()
                try:
                    resp = session.request("START")
                    if not self._is_ok(resp):
                        raise RuntimeError("START failed: {}".format(resp))
                    resp = session.request("GET {}".format(key))
                    if self._is_abort(resp):
                        last_exc = RuntimeError("GET failed: {}".format(resp))
                        if attempt < 9:
                            time.sleep(0.02)
                        continue
                    if self._is_not_found(resp):
                        return None
                    value = self._extract_value_payload(resp)
                    if value is None:
                        raise RuntimeError("GET failed: {}".format(resp))
                    return value
                finally:
                    try:
                        session.request("ABORT")
                    except Exception:
                        pass
                    session.close()
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("GET failed after retries")
        except Exception:
            if self.allow_fallback:
                return self._mini.kv_get(key)
            raise

    def start(self) -> str:
        if self._using_mini():
            return self._mini.start()
        try:
            session = self._open_session()
            resp = session.request("START")
            if not self._is_ok(resp):
                session.close()
                raise RuntimeError("START failed: {}".format(resp))
            tx_id = str(uuid.uuid4())
            self._sessions[tx_id] = session
            return tx_id
        except Exception:
            if self.allow_fallback:
                return self._mini.start()
            raise

    def get(self, tx_id: str, key: str) -> Optional[str]:
        if self._using_mini():
            return self._mini.get(tx_id, key)
        session = self._require_session(tx_id)
        resp = session.request("GET {}".format(key))
        if self._is_not_found(resp):
            return None
        value = self._extract_value_payload(resp)
        if value is None:
            raise RuntimeError("GET failed: {}".format(resp))
        return value

    def put(self, tx_id: str, key: str, value: str) -> None:
        if self._using_mini():
            self._mini.put(tx_id, key, value)
            return
        session = self._require_session(tx_id)
        resp = session.request("PUT {} {}".format(key, value))
        if not self._is_ok(resp):
            raise RuntimeError("PUT failed: {}".format(resp))

    def put_many(self, tx_id: str, requests) -> None:
        if self._using_mini():
            for key, value in requests:
                self._mini.put(tx_id, key, value)
            return
        session = self._require_session(tx_id)
        flat_parts = ["PUTMANY", str(len(requests))]
        for key, value in requests:
            flat_parts.append(key)
            flat_parts.append(self._hex_encode(value))
        resp = session.request(" ".join(flat_parts))
        if not self._is_ok(resp):
            raise RuntimeError("PUTMANY failed: {}".format(resp))

    def commit(self, tx_id: str) -> None:
        if self._using_mini():
            self._mini.commit(tx_id)
            return
        session = self._require_session(tx_id)
        try:
            resp = session.request("COMMIT")
            if not self._is_ok(resp):
                raise RuntimeError("COMMIT failed: {}".format(resp))
        finally:
            session.close()
            self._sessions.pop(tx_id, None)

    def rollback(self, tx_id: str) -> None:
        if self._using_mini():
            self._mini.rollback(tx_id)
            return
        session = self._require_session(tx_id)
        try:
            resp = session.request("ABORT")
            if not self._is_ok(resp) and "NO_ACTIVE_TXN" not in resp.upper():
                raise RuntimeError("ABORT failed: {}".format(resp))
        finally:
            session.close()
            self._sessions.pop(tx_id, None)

    def tree_start(self) -> str:
        if self._using_mini():
            return self._mini.tstart()
        try:
            session = self._open_session()
            resp = session.request("TSTART")
            if not self._is_ok(resp):
                session.close()
                raise RuntimeError("TSTART failed: {}".format(resp))
            tx_id = str(uuid.uuid4())
            self._tree_sessions[tx_id] = session
            return tx_id
        except Exception:
            if self.allow_fallback:
                return self._mini.tstart()
            raise

    def tree_branch(self, tx_id: str, parent_branch_id: int) -> int:
        if self._using_mini():
            return self._mini.tbranch(tx_id, parent_branch_id)
        session = self._require_tree_session(tx_id)
        resp = session.request("TBRANCH {}".format(parent_branch_id))
        if not self._is_ok(resp):
            raise RuntimeError("TBRANCH failed: {}".format(resp))
        parts = resp.split()
        try:
            return int(parts[-1])
        except Exception as exc:
            raise RuntimeError("failed to parse TBRANCH response: {}".format(resp)) from exc

    def tree_branch_many(self, tx_id: str, parent_branch_id: int, count: int):
        if self._using_mini():
            return self._mini.tbranch_many(tx_id, parent_branch_id, count)
        session = self._require_tree_session(tx_id)
        resp = session.request("TBRANCHMANY {} {}".format(parent_branch_id, count))
        if not self._is_ok(resp):
            raise RuntimeError("TBRANCHMANY failed: {}".format(resp))
        parts = resp.split()
        if len(parts) < 2 or parts[1].upper() != "BRANCHES":
            raise RuntimeError("failed to parse TBRANCHMANY response: {}".format(resp))
        try:
            return [int(item) for item in parts[2:]]
        except Exception as exc:
            raise RuntimeError("failed to parse TBRANCHMANY response: {}".format(resp)) from exc

    def tree_get(self, tx_id: str, branch_id: int, key: str, strict: bool = False) -> Optional[str]:
        if self._using_mini():
            return self._mini.tget(tx_id, branch_id, key, strict=strict)
        session = self._require_tree_session(tx_id)
        cmd = "TGETS" if strict else "TGET"
        resp = session.request("{} {} {}".format(cmd, branch_id, key))
        if self._is_not_found(resp):
            return None
        value = self._extract_value_payload(resp)
        if value is None:
            raise RuntimeError("{} failed: {}".format(cmd, resp))
        return value

    def tree_get_many(self, tx_id: str, requests, strict: bool = False):
        if self._using_mini():
            return self._mini.tget_many(tx_id, requests, strict=strict)
        session = self._require_tree_session(tx_id)
        cmd = "TGETSMANY" if strict else "TGETMANY"
        flat_parts = [cmd, str(len(requests))]
        for branch_id, key in requests:
            flat_parts.append(str(branch_id))
            flat_parts.append(key)
        resp = session.request(" ".join(flat_parts))
        if not self._is_ok(resp):
            raise RuntimeError("{} failed: {}".format(cmd, resp))
        parts = resp.split()
        if len(parts) < 3 or parts[1].upper() != "VALUES":
            raise RuntimeError("failed to parse {} response: {}".format(cmd, resp))
        try:
            count = int(parts[2])
        except Exception as exc:
            raise RuntimeError("failed to parse {} response: {}".format(cmd, resp)) from exc
        expected_len = 3 + count * 2
        if len(parts) != expected_len:
            raise RuntimeError("bad {} response arity: {}".format(cmd, resp))
        values = []
        for idx in range(count):
            encoded = parts[4 + idx * 2]
            values.append(self._hex_decode(encoded))
        return values

    def tree_put(self, tx_id: str, branch_id: int, key: str, value: str) -> None:
        if self._using_mini():
            self._mini.tput(tx_id, branch_id, key, value)
            return
        session = self._require_tree_session(tx_id)
        resp = session.request("TPUT {} {} {}".format(branch_id, key, value))
        if not self._is_ok(resp):
            raise RuntimeError("TPUT failed: {}".format(resp))

    def tree_put_many(self, tx_id: str, requests) -> None:
        if self._using_mini():
            self._mini.tput_many(tx_id, requests)
            return
        session = self._require_tree_session(tx_id)
        flat_parts = ["TPUTMANY", str(len(requests))]
        for branch_id, key, value in requests:
            flat_parts.append(str(branch_id))
            flat_parts.append(key)
            flat_parts.append(self._hex_encode(value))
        resp = session.request(" ".join(flat_parts))
        if not self._is_ok(resp):
            raise RuntimeError("TPUTMANY failed: {}".format(resp))

    def tree_select_winner(self, tx_id: str, branch_id: int) -> None:
        if self._using_mini():
            self._mini.twinner(tx_id, branch_id)
            return
        session = self._require_tree_session(tx_id)
        resp = session.request("TWINNER {}".format(branch_id))
        if not self._is_ok(resp):
            raise RuntimeError("TWINNER failed: {}".format(resp))

    def tree_refresh_winner(self, tx_id: str, branch_id: int, key: str) -> Optional[str]:
        if self._using_mini():
            return self._mini.trefresh_winner(tx_id, branch_id, key)
        session = self._require_tree_session(tx_id)
        resp = session.request("TREFRESH_WINNER {} {}".format(branch_id, key))
        if self._is_not_found(resp):
            return None
        value = self._extract_value_payload(resp)
        if value is None:
            raise RuntimeError("TREFRESH_WINNER failed: {}".format(resp))
        return value

    def tree_commit(self, tx_id: str) -> None:
        if self._using_mini():
            self._mini.tcommit(tx_id)
            return
        session = self._require_tree_session(tx_id)
        try:
            resp = session.request("TCOMMIT")
            if not self._is_ok(resp):
                raise RuntimeError("TCOMMIT failed: {}".format(resp))
        finally:
            session.close()
            self._tree_sessions.pop(tx_id, None)

    def tree_commit_retry(self, tx_id: str) -> Dict[str, Any]:
        if self._using_mini():
            self._mini.tcommit_retry(tx_id)
            return {"ok": True, "committed": True, "abort_reason": ""}
        session = self._require_tree_session(tx_id)
        try:
            resp = session.request("TCOMMIT_RETRY")
            if self._is_ok(resp):
                return {"ok": True, "committed": True, "abort_reason": ""}
            if self._is_abort(resp):
                parts = resp.split(" ", 2)
                reason = parts[2] if len(parts) >= 3 else ""
                return {"ok": False, "committed": False, "abort_reason": reason, "response": resp}
            raise RuntimeError("TCOMMIT_RETRY failed: {}".format(resp))
        finally:
            if tx_id not in self._tree_sessions:
                return
            if "OK" in locals().get("resp", "").upper():
                session.close()
                self._tree_sessions.pop(tx_id, None)

    def tree_abort(self, tx_id: str) -> None:
        if self._using_mini():
            self._mini.tabort(tx_id)
            return
        session = self._require_tree_session(tx_id)
        try:
            resp = session.request("ABORT")
            if not self._is_ok(resp) and "NO_ACTIVE_TREE_TXN" not in resp.upper() and "NO_ACTIVE_TXN" not in resp.upper():
                raise RuntimeError("tree ABORT failed: {}".format(resp))
        finally:
            session.close()
            self._tree_sessions.pop(tx_id, None)

    def dump_all(self) -> Dict[str, Any]:
        if self._using_mini():
            return self._mini.dump_all()
        return {
            "ok": False,
            "error": "kv_dump is not supported by the Tree-DB socket server",
            "backend": "socket",
            "hint": "如需本地调试全量 KV，可设置 TREE_DB_BACKEND=mini 或后续补一个服务端 DUMP 命令。",
        }


agent_db = AgentDBClient()
