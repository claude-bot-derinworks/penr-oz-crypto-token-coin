import os

import httpx
import pytest

# Service URLs configurable through environment variables
WALLET_SERVICE_URL = os.getenv("WALLET_SERVICE_URL", "http://localhost:8000")
TRANSACTION_SERVICE_URL = os.getenv("TRANSACTION_SERVICE_URL", "http://localhost:8001")
BLOCKCHAIN_SERVICE_URL = os.getenv("BLOCKCHAIN_SERVICE_URL", "http://localhost:8002")
MINER_SERVICE_URL = os.getenv("MINER_SERVICE_URL", "http://localhost:8003")

MINING_REWARD = 50.0


@pytest.mark.integration
class TestEndToEndHappyPath:
    """End-to-end integration test: Wallet -> Transaction -> Miner -> Blockchain"""

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
            resp = client.get(f"{TRANSACTION_SERVICE_URL}/transaction/pending")
            assert resp.status_code == 200
            pending = resp.json()["transactions"]
            assert (
                len(pending) >= 1
            ), "Pending pool should contain at least 1 transaction"

            matching = [
                tx
                for tx in pending
                if tx["sender"] == wallet_a
                and tx["receiver"] == wallet_b
                and tx["amount"] == tx_amount
            ]
            assert (
                len(matching) == 1
            ), f"Expected 1 matching pending transaction, found {len(matching)}"

            # Record blockchain length before mining
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain")
            assert resp.status_code == 200
            chain_length_before = resp.json()["length"]

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

            resp = client.get(f"{TRANSACTION_SERVICE_URL}/transaction/pending")
            assert resp.status_code == 200
            pending_after = resp.json()["transactions"]
            assert len(pending_after) == 0, (
                f"Pending pool should be empty after mining, "
                f"found {len(pending_after)} transactions"
            )

            # ----------------------------------------------------------
            # Step 6: Validate balances
            # ----------------------------------------------------------
            miner_address = os.getenv("MINER_ADDRESS", "MINER_REWARD_ADDRESS")

            # Miner should have received the mining reward
            resp = client.get(
                f"{BLOCKCHAIN_SERVICE_URL}/blockchain/balance/{miner_address}"
            )
            assert resp.status_code == 200
            miner_balance = resp.json()["balance"]
            assert miner_balance == MINING_REWARD, (
                f"Miner balance should be {MINING_REWARD}, " f"got {miner_balance}"
            )

            # Wallet A sent tx_amount
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain/balance/{wallet_a}")
            assert resp.status_code == 200
            balance_a = resp.json()["balance"]
            assert (
                balance_a == -tx_amount
            ), f"Wallet A balance should be {-tx_amount}, got {balance_a}"

            # Wallet B received tx_amount
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain/balance/{wallet_b}")
            assert resp.status_code == 200
            balance_b = resp.json()["balance"]
            assert (
                balance_b == tx_amount
            ), f"Wallet B balance should be {tx_amount}, got {balance_b}"

            # Blockchain integrity check
            resp = client.get(f"{BLOCKCHAIN_SERVICE_URL}/blockchain/validate")
            assert resp.status_code == 200
            assert (
                resp.json()["valid"] is True
            ), "Blockchain should be valid after mining"
