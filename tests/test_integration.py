import os
import time

import httpx
import pytest

from shared.constants import MINING_REWARD

# Service URLs configurable through environment variables
WALLET_SERVICE_URL = os.getenv("WALLET_SERVICE_URL", "http://localhost:8000")
TRANSACTION_SERVICE_URL = os.getenv("TRANSACTION_SERVICE_URL", "http://localhost:8001")
BLOCKCHAIN_SERVICE_URL = os.getenv("BLOCKCHAIN_SERVICE_URL", "http://localhost:8002")
MINER_SERVICE_URL = os.getenv("MINER_SERVICE_URL", "http://localhost:8003")


@pytest.mark.integration
class TestEndToEndHappyPath:
    """End-to-end integration test: Wallet -> Transaction -> Miner -> Blockchain"""

    def _get_balance(self, client: httpx.Client, address: str) -> float:
        resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain/balance/{address}")
        resp.raise_for_status()
        return resp.json()["balance"]

    def test_full_flow(self):
        """
        Validate that all four microservices work together correctly.

        Flow:
        1. Create two wallets via Wallet Service
        2. Submit a transaction between them via Transaction Service
        3. Verify the transaction appears in the pending pool
        4. Trigger mining via Miner Service
        5. Confirm blockchain grew by one block and pending pool cleared
        6. Validate balances reflect both the transaction and mining rewards
        """
        with httpx.Client(timeout=30.0) as client:
            # ----------------------------------------------------------
            # Step 1: Create two wallets
            # ----------------------------------------------------------
            resp = client.post(f"{WALLET_SERVICE_URL}/wallet/create")
            assert resp.status_code == 201, f"Failed to create wallet A: {resp.text}"
            wallet_a = resp.json()["address"]

            resp = client.post(f"{WALLET_SERVICE_URL}/wallet/create")
            assert resp.status_code == 201, f"Failed to create wallet B: {resp.text}"
            wallet_b = resp.json()["address"]

            assert wallet_a != wallet_b, "Wallet addresses must be unique"

            # ----------------------------------------------------------
            # Step 2: Submit a transaction from wallet A to wallet B
            # ----------------------------------------------------------
            tx_amount = 10.0
            resp = client.post(
                f"{TRANSACTION_SERVICE_URL}/transaction/send",
                json={
                    "sender": wallet_a,
                    "receiver": wallet_b,
                    "amount": tx_amount,
                },
            )
            assert resp.status_code == 200, f"Failed to send transaction: {resp.text}"
            assert resp.json()["status"] == "pending"

            # ----------------------------------------------------------
            # Step 3: Verify the transaction appears in the pending pool
            # ----------------------------------------------------------
            # Poll until the transaction appears in the pending pool
            deadline = time.time() + 10  # 10-second timeout
            tx_in_pool = False
            while time.time() < deadline:
                resp = client.get(f"{TRANSACTION_SERVICE_URL}/transaction/pending")
                assert resp.status_code == 200, (
                    f"Failed to get pending txs: {resp.text}"
                )
                pending_txs = resp.json()["transactions"]
                matching = [
                    tx
                    for tx in pending_txs
                    if tx["sender"] == wallet_a
                    and tx["receiver"] == wallet_b
                    and tx["amount"] == tx_amount
                ]
                if len(matching) == 1:
                    tx_in_pool = True
                    break
                time.sleep(0.5)

            assert tx_in_pool, (
                f"Transaction from {wallet_a} to {wallet_b} "
                "did not appear in pending pool within timeout"
            )

            # Record blockchain length before mining
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain")
            assert resp.status_code == 200
            chain_length_before = resp.json()["length"]

            # Get miner address from the running miner service
            resp = client.get(f"{MINER_SERVICE_URL}/miner/stats")
            assert resp.status_code == 200, f"Failed to get miner stats: {resp.text}"
            miner_address = resp.json()["miner_address"]

            # Record balances before mining to compute deltas
            miner_balance_before = self._get_balance(client, miner_address)
            balance_a_before = self._get_balance(client, wallet_a)
            balance_b_before = self._get_balance(client, wallet_b)

            # ----------------------------------------------------------
            # Step 4: Trigger mining via Miner Service
            # ----------------------------------------------------------
            resp = client.post(f"{MINER_SERVICE_URL}/mine", timeout=120.0)
            assert resp.status_code == 200, f"Mining failed: {resp.text}"
            mine_result = resp.json()
            assert (
                mine_result["status"] == "success"
            ), f"Mining did not succeed: {mine_result}"

            # ----------------------------------------------------------
            # Step 5: Confirm blockchain grew and pending pool cleared
            # ----------------------------------------------------------
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain")
            assert resp.status_code == 200
            chain_length_after = resp.json()["length"]
            assert chain_length_after == chain_length_before + 1, (
                f"Blockchain should grow by 1 block: "
                f"before={chain_length_before}, after={chain_length_after}"
            )

            # Verify our specific transaction is no longer pending
            resp = client.get(f"{TRANSACTION_SERVICE_URL}/transaction/pending")
            assert resp.status_code == 200
            pending_after = resp.json()["transactions"]
            still_pending = [
                tx
                for tx in pending_after
                if tx["sender"] == wallet_a
                and tx["receiver"] == wallet_b
                and tx["amount"] == tx_amount
            ]
            assert len(still_pending) == 0, (
                "Our transaction should no longer be in the pending pool "
                "after mining"
            )

            # ----------------------------------------------------------
            # Step 6: Validate balances (using deltas)
            # ----------------------------------------------------------

            # Miner should have received the mining reward
            miner_balance_after = self._get_balance(client, miner_address)
            miner_delta = miner_balance_after - miner_balance_before
            assert miner_delta == MINING_REWARD, (
                f"Miner balance should increase by {MINING_REWARD}, "
                f"got delta {miner_delta}"
            )

            # Wallet A sent tx_amount
            balance_a_after = self._get_balance(client, wallet_a)
            delta_a = balance_a_after - balance_a_before
            assert delta_a == -tx_amount, (
                f"Wallet A balance should decrease by {tx_amount}, "
                f"got delta {delta_a}"
            )

            # Wallet B received tx_amount
            balance_b_after = self._get_balance(client, wallet_b)
            delta_b = balance_b_after - balance_b_before
            assert delta_b == tx_amount, (
                f"Wallet B balance should increase by {tx_amount}, "
                f"got delta {delta_b}"
            )

            # Blockchain integrity check
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain/validate")
            assert resp.status_code == 200
            assert (
                resp.json()["valid"] is True
            ), "Blockchain should be valid after mining"
