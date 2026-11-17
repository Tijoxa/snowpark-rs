import json
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

import pytest

import snowpark_rs
from snowpark_rs import DataFrame, _session
from snowpark_rs.functions import col, lit


def test_offline_api_does_not_import_the_optional_backend(monkeypatch):
    def missing(name):
        raise ModuleNotFoundError(name=name)

    monkeypatch.setattr(_session, "import_module", missing)
    frame = DataFrame.table("DB.SCHEMA.EXAMPLE")
    assert frame.to_sql()[1] == []
    with pytest.raises(ImportError, match=r"snowpark-rs\[session\]"):
        _ = snowpark_rs.Session


def test_session_arrow_end_to_end(tmp_path, monkeypatch):
    pyarrow = pytest.importorskip("pyarrow")
    pytest.importorskip("_snowpark_rs_session")
    from snowpark_rs import Session

    queries = []
    downloads = []
    rowtype = [
        {"name": "AMOUNT", "nullable": False, "type": "fixed", "precision": 38, "scale": 2},
        {"name": "FLAG", "nullable": True, "type": "boolean"},
        {"name": "TIME", "nullable": True, "type": "timestamp_tz", "scale": 3},
    ]
    initial = [["123456789012345678901234567890.12", None, "0.000 1500"]]
    remote = json.dumps(["-0.25", "true", None]).encode()

    class Handler(BaseHTTPRequestHandler):
        def respond(self, body):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if urlsplit(self.path).path == "/session/v1/login-request":
                name = body["data"]["LOGIN_NAME"]
                response = {"success": True, "data": {"token": f"token-{name}"}}
            else:
                authorization = self.headers["Authorization"]
                queries.append((authorization, body))
                data: dict[str, object] = {
                    "queryId": "test-query",
                    "queryResultFormat": "json",
                    "rowtype": rowtype,
                }
                if "token-empty" in authorization:
                    data["rowset"] = []
                else:
                    data["rowset"] = initial
                    path = "/error" if "token-broken" in authorization else "/batch"
                    data["chunks"] = [
                        {
                            "url": f"http://127.0.0.1:{server.server_port}{path}",
                            "rowCount": 1,
                            "compressedSize": len(remote),
                            "uncompressedSize": len(remote),
                        }
                    ]
                response = {"success": True, "data": data}
            self.respond(json.dumps(response).encode())

        def do_GET(self):
            downloads.append(self.path)
            if self.path == "/error":
                self.send_error(403)
            else:
                self.respond(remote)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            profiles = "".join(
                f'[{name}]\naccount = "mock"\nuser = "{name}"\npassword = "fake"\n'
                f'host = "127.0.0.1"\nprotocol = "http"\nport = {server.server_port}\n\n'
                for name in ("data", "empty", "broken")
            )
            (tmp_path / "connections.toml").write_text(profiles, encoding="utf-8")
            monkeypatch.setenv("SNOWFLAKE_HOME", str(tmp_path))

            with pytest.raises(RuntimeError, match="not found"):
                Session.builder.config("connection_name", "missing").create()
            session = Session.builder.config("connection_name", "data").create()
            empty_session = Session.builder.config("connection_name", "empty").create()
            broken_session = Session.builder.config("connection_name", "broken").create()
            frame = DataFrame.table("DB.SCHEMA.EXAMPLE").filter(col("FLAG").eq(lit(True)))

            iterator = frame.to_arrow_batches(session)
            assert downloads == []
            first = next(iterator)
            assert downloads == []
            batches = [first, *iterator]
            assert downloads == ["/batch"]
            assert first["AMOUNT"].to_pylist() == [Decimal("123456789012345678901234567890.12")]
            assert first["TIME"][0].as_py()["offset_seconds"] == 3600

            result = frame.to_arrow(session)
            assert isinstance(result, pyarrow.Table)
            assert result.equals(pyarrow.concat_tables(batches))
            assert result["AMOUNT"].to_pylist()[-1] == Decimal("-0.25")
            assert result["FLAG"].to_pylist() == [None, True]
            assert result["TIME"][1].as_py() is None
            empty = frame.to_arrow(empty_session)
            assert empty.num_rows == 0
            assert empty.schema == result.schema
            assert list(frame.to_arrow_batches(empty_session)) == []

            early = frame.to_arrow_batches(
                Session.builder.config("connection_name", "data").create()
            )
            assert next(early).num_rows == 1
            downloads_before_drop = list(downloads)
            del early
            assert downloads == downloads_before_drop

            broken = frame.to_arrow_batches(broken_session)
            assert next(broken).num_rows == 1
            with pytest.raises(RuntimeError, match="result read failed.*test-query"):
                next(broken)
            assert list(broken) == []

            assert all(body["sqlText"] == frame.to_sql()[0] for _, body in queries)
            assert all(body["bindings"]["1"]["type"] == "BOOLEAN" for _, body in queries)
        finally:
            server.shutdown()
            worker.join()
