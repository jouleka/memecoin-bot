"""Helius JSON-RPC reads for the safety gate (spec §5.3 checks 1/2/4).

getTokenLargestAccounts hits intermittent -32603 "account index service overloaded"
(observed in M0 recon) — wrapped in bounded retry; exhaustion raises RpcError, which
the gate treats as check-unavailable (fail-closed, M3 delta 3/4)."""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass

import httpx


class RpcError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class MintInfo:
    mint_authority: str | None
    freeze_authority: str | None
    supply: int
    decimals: int


@dataclass(frozen=True, slots=True)
class Holder:
    address: str
    amount: int


@dataclass(frozen=True, slots=True)
class SignatureInfo:
    signature: str
    slot: int
    block_time: int | None


class SafetyRpc:
    def __init__(self, rpc_url: str, *, client: httpx.AsyncClient,
                 max_retries: int = 3, backoff_base_s: float = 0.3) -> None:
        self._url = rpc_url
        self._client = client
        self._max_retries = max_retries
        self._backoff = backoff_base_s

    async def _call(self, method: str, params: list) -> dict:
        body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        last: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await self._client.post(self._url, json=body, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    raise RpcError(f"{method}: {data['error']}")
                return data["result"]
            except (httpx.HTTPError, RpcError, KeyError) as exc:
                last = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(self._backoff * (2 ** attempt) * (0.5 + random.random())
                                        if self._backoff else 0)
        raise RpcError(f"{method} failed after {self._max_retries} attempts: {last}")

    async def mint_info(self, mint: str) -> MintInfo:
        result = await self._call("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
        try:
            info = result["value"]["data"]["parsed"]["info"]
            return MintInfo(
                mint_authority=info.get("mintAuthority"),
                freeze_authority=info.get("freezeAuthority"),
                supply=int(info["supply"]),
                decimals=int(info["decimals"]),
            )
        except (KeyError, TypeError, IndexError, ValueError) as exc:
            raise RpcError(f"malformed getAccountInfo response: {exc}") from exc

    async def largest_accounts(self, mint: str) -> list[Holder]:
        result = await self._call("getTokenLargestAccounts", [mint])
        try:
            return [Holder(address=v["address"], amount=int(v["amount"]))
                    for v in result["value"]]
        except (KeyError, TypeError, IndexError, ValueError) as exc:
            raise RpcError(f"malformed getTokenLargestAccounts response: {exc}") from exc

    async def account_owners(self, addresses: list[str]) -> dict[str, str]:
        """Map each TOKEN-ACCOUNT address -> its owner pubkey (getMultipleAccounts,
        jsonParsed). getTokenLargestAccounts returns token-account addresses, NOT owners;
        the safety gate needs owners to exclude protocol-owned reserves (the bonding
        curve's own token account holds ~80% of supply pre-graduation). Accounts that are
        absent/closed or not a parseable SPL token account are omitted from the map
        (conservative: an un-classifiable account is left IN the concentration count, not
        silently excluded). A malformed envelope raises RpcError -> gate fails closed."""
        if not addresses:
            return {}                       # never spend a call on an empty batch
        result = await self._call(
            "getMultipleAccounts", [list(addresses), {"encoding": "jsonParsed"}])
        try:
            pairs = list(zip(addresses, result["value"], strict=True))
        except (KeyError, TypeError, ValueError) as exc:
            raise RpcError(f"malformed getMultipleAccounts response: {exc}") from exc
        owners: dict[str, str] = {}
        for addr, val in pairs:
            try:
                owners[addr] = val["data"]["parsed"]["info"]["owner"]
            except (KeyError, TypeError, IndexError):
                continue                    # missing / non-jsonParsed / not a token account
        return owners

    async def signatures_for_address(self, address: str, *, limit: int) -> list[SignatureInfo]:
        """Bounded `getSignaturesForAddress` wrapper for candidate-only early-buyer reads."""
        result = await self._call("getSignaturesForAddress", [address, {"limit": limit}])
        try:
            return [SignatureInfo(signature=str(row["signature"]), slot=int(row["slot"]),
                                  block_time=(None if row.get("blockTime") is None
                                              else int(row["blockTime"])))
                    for row in result]
        except (KeyError, TypeError, ValueError) as exc:
            raise RpcError(f"malformed getSignaturesForAddress response: {exc}") from exc

    async def transaction_logs(self, signature: str) -> list[str]:
        """Return Solana transaction `meta.logMessages` for a signature.

        Null/malformed results are RpcError so the P2 early-buyer gate can fail closed.
        """
        result = await self._call(
            "getTransaction", [signature, {"encoding": "json",
                                           "maxSupportedTransactionVersion": 0}])
        try:
            logs = result["meta"]["logMessages"]
            if not isinstance(logs, list):
                raise TypeError("logMessages is not a list")
            return [str(x) for x in logs]
        except (KeyError, TypeError, ValueError) as exc:
            raise RpcError(f"malformed getTransaction response: {exc}") from exc
