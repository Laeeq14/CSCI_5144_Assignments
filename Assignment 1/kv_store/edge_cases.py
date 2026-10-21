"""
edge_cases.py -- Correctness tests for the KV store.

Every test uses a unique key suffix so results are independent of prior runs
and the test suite can be re-run cleanly any number of times.

Tests 1-13: original correctness suite
Tests 14-16: new *string value validation (absent vs empty)
"""
import http.client
import json
import uuid

conn = http.client.HTTPConnection('127.0.0.1', 8080)

# Unique suffix per run -- no leftover-state dependence
RUN = uuid.uuid4().hex[:8]
PASSED = FAILED = 0


def req(method, path, body=None, ct='application/json'):
    headers = {}
    if body is not None:
        headers['Content-Type'] = ct
        headers['Content-Length'] = str(len(body))
    conn.request(method, path, body=body, headers=headers)
    r = conn.getresponse()
    data = r.read()
    return r.status, data.decode()


def check(label, status, body, expect_status, expect_in=None):
    global PASSED, FAILED
    ok = (status == expect_status)
    if expect_in and ok:
        ok = (expect_in in body)
    marker = "PASS" if ok else "FAIL"
    if ok:
        PASSED += 1
    else:
        FAILED += 1
    print(f"  [{marker}] {label}: HTTP {status}  body={body[:90]}")


def inc(ok):
    global PASSED, FAILED
    if ok:
        PASSED += 1
    else:
        FAILED += 1
    return ok


def key(name):
    return f"ec_{RUN}_{name}"


print(f"Edge-case tests  (run suffix: {RUN})")
print()

# -- 1. Normal PUT (new) -> 201 -----------------------------------------------
s, b = req('POST', f'/{key("a")}', b'{"value":"world"}')
check("PUT new key -> 201", s, b, 201, '"created":true')

# -- 2. GET existing -> 200 + correct value -----------------------------------
s, b = req('GET', f'/{key("a")}')
check("GET existing -> 200 + correct value", s, b, 200, '"world"')

# -- 3. PUT update (same key) -> 200 + created:false -------------------------
s, b = req('POST', f'/{key("a")}', b'{"value":"updated"}')
check("PUT update -> 200 + created:false", s, b, 200, '"created":false')

# -- 4. GET after update ------------------------------------------------------
s, b = req('GET', f'/{key("a")}')
check("GET after update -> updated value", s, b, 200, '"updated"')

# -- 5. DELETE existing -> 200 ------------------------------------------------
s, b = req('DELETE', f'/{key("a")}')
check("DELETE existing -> 200", s, b, 200, '"message"')

# -- 6. GET deleted key -> 404 ------------------------------------------------
s, b = req('GET', f'/{key("a")}')
check("GET deleted key -> 404", s, b, 404, '"error"')

# -- 7. DELETE already-deleted -> 404 -----------------------------------------
s, b = req('DELETE', f'/{key("a")}')
check("DELETE missing key -> 404", s, b, 404, '"error"')

# -- 8. GET never-written key -> 404 ------------------------------------------
s, b = req('GET', f'/{key("never")}')
check("GET never-written key -> 404", s, b, 404, '"error"')

# -- 9. Empty key -> 400 ------------------------------------------------------
s, b = req('POST', '/', b'{"value":"x"}')
check("POST empty key -> 400", s, b, 400, '"error"')

# -- 10. Malformed JSON body -> 400 -------------------------------------------
s, b = req('POST', f'/{key("bad")}', b'not json at all')
check("POST bad JSON -> 400", s, b, 400, '"error"')

# -- 11. Wrong HTTP method -> 405 ---------------------------------------------
s, b = req('PATCH', f'/{key("patch")}')
check("PATCH (unsupported) -> 405", s, b, 405)

# -- 12. Value with special characters (JSON escaping) ------------------------
special_val = 'hello "world"\n\ttab\\slash\u00e9clair'
body = json.dumps({'value': special_val}).encode()
s, b = req('POST', f'/{key("special")}', body)
check("PUT special chars -> 201", s, b, 201)

s, b = req('GET', f'/{key("special")}')
got = json.loads(b).get('value') if s == 200 else None
ok_rt = inc(got == special_val)
print(f"  [{'PASS' if ok_rt else 'FAIL'}] GET special chars round-trip: got={got!r}")

# -- 13. Large value (900 KB, just under 1 MiB limit) ------------------------
big_val = 'x' * (900 * 1024)
body = json.dumps({'value': big_val}).encode()
s, b = req('POST', f'/{key("big")}', body)
check("PUT 900 KB value -> 201", s, b, 201)

s, b = req('GET', f'/{key("big")}')
got_len = len(json.loads(b).get('value', '')) if s == 200 else 0
ok_big = inc(got_len == len(big_val))
print(f"  [{'PASS' if ok_big else 'FAIL'}] GET 900 KB value: got {got_len} chars, expected {len(big_val)}")

# -- 14. Missing value field ({}) -> 400 (new: *string distinguishes absent) --
s, b = req('POST', f'/{key("missing")}', b'{}')
check("POST missing value field -> 400", s, b, 400, '"error"')

# -- 15. Wrong value type (number) -> 400 -------------------------------------
s, b = req('POST', f'/{key("wrongtype")}', b'{"value":42}')
check("POST value=42 (number) -> 400", s, b, 400, '"error"')

# -- 16. Empty string value -> 201 (absent != empty, empty string is valid) ---
s, b = req('POST', f'/{key("empty")}', b'{"value":""}')
check("POST empty string value -> 201", s, b, 201)
req('DELETE', f'/{key("empty")}')

# -- Cleanup ------------------------------------------------------------------
for name in ('special', 'big'):
    req('DELETE', f'/{key(name)}')

conn.close()
print()
print(f"Result: {PASSED} passed, {FAILED} failed out of {PASSED+FAILED} tests")
if FAILED:
    raise SystemExit(1)
