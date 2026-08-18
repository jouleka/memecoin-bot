import json

import httpx
import pytest

from memebot.safety.rpc import RpcError, SafetyRpc, SignatureInfo

MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"


def account_info(mint_authority, freeze_authority):
    return {"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": {"parsed": {"info": {
        "mintAuthority": mint_authority, "freezeAuthority": freeze_authority,
        "supply": "1000000000000000", "decimals": 6}}, "program": "spl-token"}}}}


def largest(amounts):
    return {"jsonrpc": "2.0", "id": 1, "result": {"value": [
        {"address": f"H{i}", "amount": str(a), "decimals": 6} for i, a in enumerate(amounts)]}}


def transport(routes):
    """routes: method -> list of httpx.Response (popped per call, last repeats)."""
    calls = []

    def handler(request):
        body = json.loads(request.content)
        calls.append(body["method"])
        seq = routes[body["method"]]
        return seq.pop(0) if len(seq) > 1 else seq[0]

    return httpx.MockTransport(handler), calls


async def test_authorities_revoked_parsed():
    t, _ = transport({"getAccountInfo": [httpx.Response(200, json=account_info(None, None))]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    info = await rpc.mint_info(MINT)
    assert info.mint_authority is None and info.freeze_authority is None
    assert info.supply == 1000000000000000 and info.decimals == 6


async def test_authorities_present():
    t, _ = transport({"getAccountInfo": [httpx.Response(200, json=account_info("SOMEKEY", "FRZ"))]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    info = await rpc.mint_info(MINT)
    assert info.mint_authority == "SOMEKEY" and info.freeze_authority == "FRZ"


async def test_largest_accounts_ok():
    t, _ = transport({"getTokenLargestAccounts": [httpx.Response(200, json=largest([500, 300, 200]))]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    holders = await rpc.largest_accounts(MINT)
    assert [h.amount for h in holders] == [500, 300, 200]
    assert holders[0].address == "H0"


async def test_largest_accounts_retries_on_32603_then_succeeds():
    err = httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
                                    "error": {"code": -32603, "message": "account index service overloaded"}})
    ok = httpx.Response(200, json=largest([1]))
    t, calls = transport({"getTokenLargestAccounts": [err, err, ok]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t),
                    max_retries=3, backoff_base_s=0.0)
    holders = await rpc.largest_accounts(MINT)
    assert [h.amount for h in holders] == [1]
    assert calls.count("getTokenLargestAccounts") == 3   # 2 retries then success


async def test_largest_accounts_raises_after_exhausting_retries():
    err = httpx.Response(200, json={"jsonrpc": "2.0", "id": 1,
                                    "error": {"code": -32603, "message": "overloaded"}})
    t, _ = transport({"getTokenLargestAccounts": [err]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t),
                    max_retries=2, backoff_base_s=0.0)
    with pytest.raises(RpcError):
        await rpc.largest_accounts(MINT)


async def test_mint_info_null_value_raises_rpcerror():
    # getAccountInfo returns value=null for a nonexistent/closed account
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": None}}
    t, _ = transport({"getAccountInfo": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):
        await rpc.mint_info(MINT)


async def test_mint_info_unparsed_base64_raises_rpcerror():
    # non-jsonParsed data (Token-2022 / unrecognized program) → data is a [b64, "base64"] list
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": {"data": ["AQID", "base64"]}}}
    t, _ = transport({"getAccountInfo": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):
        await rpc.mint_info(MINT)


async def test_largest_accounts_malformed_value_raises_rpcerror():
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": None}}
    t, _ = transport({"getTokenLargestAccounts": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):
        await rpc.largest_accounts(MINT)


def multi_accounts(owner_by_addr, order):
    """getMultipleAccounts jsonParsed envelope: value aligned to `order`; None if absent."""
    value = [({"data": {"parsed": {"info": {"owner": owner_by_addr[a]}}}}
              if a in owner_by_addr else None) for a in order]
    return {"jsonrpc": "2.0", "id": 1, "result": {"value": value}}


async def test_account_owners_maps_address_to_owner():
    order = ["ATA_CURVE", "ATA_WALLET"]
    body = multi_accounts({"ATA_CURVE": "CURVE", "ATA_WALLET": "WALLET"}, order)
    t, _ = transport({"getMultipleAccounts": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    assert await rpc.account_owners(order) == {"ATA_CURVE": "CURVE", "ATA_WALLET": "WALLET"}


async def test_account_owners_skips_absent_and_unparseable():
    order = ["ATA_OK", "ATA_GONE", "ATA_WEIRD"]
    value = [
        {"data": {"parsed": {"info": {"owner": "OWN"}}}},
        None,                                    # closed/absent account
        {"data": ["b64blob", "base64"]},         # non-jsonParsed (e.g. Token-2022) -> skip
    ]
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": value}}
    t, _ = transport({"getMultipleAccounts": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    assert await rpc.account_owners(order) == {"ATA_OK": "OWN"}   # only the parseable one


async def test_account_owners_empty_input_makes_no_call():
    t, calls = transport({"getMultipleAccounts": [httpx.Response(200, json={"result": {"value": []}})]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    assert await rpc.account_owners([]) == {}
    assert calls == []                           # empty batch must not spend a credit


async def test_account_owners_null_value_raises_rpcerror():
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": None}}
    t, _ = transport({"getMultipleAccounts": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):                # malformed envelope -> fail-closed upstream
        await rpc.account_owners(["A"])


async def test_account_owners_length_mismatch_raises_rpcerror():
    # value shorter than the requested addresses -> strict zip catches the desync
    body = {"jsonrpc": "2.0", "id": 1, "result": {"value": [{"data": {"parsed": {"info": {"owner": "O"}}}}]}}
    t, _ = transport({"getMultipleAccounts": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):
        await rpc.account_owners(["A", "B"])


async def test_signatures_for_address_parses_and_passes_limit():
    body = {"jsonrpc": "2.0", "id": 1, "result": [
        {"signature": "SIG1", "slot": 10, "blockTime": 1000},
        {"signature": "SIG2", "slot": 9, "blockTime": None},
    ]}
    seen_params = []

    def handler(request):
        payload = json.loads(request.content)
        seen_params.append(payload["params"])
        return httpx.Response(200, json=body)

    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    rows = await rpc.signatures_for_address("CURVE", limit=2)
    assert rows == [SignatureInfo(signature="SIG1", slot=10, block_time=1000),
                    SignatureInfo(signature="SIG2", slot=9, block_time=None)]
    assert seen_params == [["CURVE", {"limit": 2}]]


async def test_transaction_logs_parses_meta_log_messages():
    body = {"jsonrpc": "2.0", "id": 1, "result": {"meta": {"logMessages": ["a", "b"]}}}
    t, _ = transport({"getTransaction": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    assert await rpc.transaction_logs("SIG") == ["a", "b"]


async def test_transaction_logs_null_result_raises_rpcerror():
    body = {"jsonrpc": "2.0", "id": 1, "result": None}
    t, _ = transport({"getTransaction": [httpx.Response(200, json=body)]})
    rpc = SafetyRpc("https://rpc.test", client=httpx.AsyncClient(transport=t))
    with pytest.raises(RpcError):
        await rpc.transaction_logs("SIG")
