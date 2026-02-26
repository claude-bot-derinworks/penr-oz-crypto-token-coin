import os
import time

import httpx
import pytest

from shared.constants import MINING_REWARD

# Service URLs configurable through environment variables
WALLET_SERVICE_URL = os.getenv(
    "WALLET_SERVICE_URL", "http://localhost:8000"
)
TRANSACTION_SERVICE_URL = os.getenv(
    "TRANSACTION_SERVICE_URL", "http://localhost:8001"
)
BLOCKCHAIN_SERVICE_URL = os.getenv(
    "BLOCKCHAIN_SERVICE_URL", "http://localhost:8002"
)
MINER_SERVICE_URL = os.getenv(
    "MINER_SERVICE_URL", "http://localhost:8003"
)

# API endpoint paths
WALLET_CREATE_PATH = "/wallet/create"
TRANSACTION_SEND_PATH = "/transaction/send"
TRANSACTION_PENDING_PATH = "/transaction/pending"
BLOCKCHAIN_PATH = "/blockchain"
BLOCKCHAIN_BALANCE_PATH = "/blockchain/balance"
BLOCKCHAIN_VALIDATE_PATH = "/blockchain/validate"
MINER_STATS_PATH = "/miner/stats"
MINE_PATH = "/mine"

# Polling configuration
POLL_TIMEOUT_S = 10
POLL_INTERVAL_S = 0.5


@pytest.mark.integration
class TestEndToEndHappyPath:
    """End-to-end integration test: Wallet -> Transaction -> Miner -> Blockchain"""

    def _get_balance(
        self, client: httpx.Client, address: str
    ) -> float:
        resp = client.get(
            f"{BLOCKCHAIN_SERVICE_URL}{BLOCKCHAIN_BALANCE_PATH}"
            f"/{address}"
        )
        resp.raise_for_status()
        return resp.json()["balance"]

    def _create_wallets(
        self, client: httpx.Client
    ) -> tuple[str, str]:
        resp = client.post(
            f"{WALLET_SERVICE_URL}{WALLET_CREATE_PATH}"
        )
        assert resp.status_code == 201, (
            f"Failed to create wallet A: {resp.text}"
        )
        wallet_a = resp.json()["address"]

        resp = client.post(
            f"{WALLET_SERVICE_URL}{WALLET_CREATE_PATH}"
        )
        assert resp.status_code == 201, (
            f"Failed to create wallet B: {resp.text}"
        )
        wallet_b = resp.json()["address"]

        assert wallet_a != wallet_b, (
            "Wallet addresses must be unique"
        )
        return wallet_a, wallet_b

    def _submit_transaction(
        self,
        client: httpx.Client,
        sender: str,
        receiver: str,
        amount: float,
    ) -> None:
        resp = client.post(
            f"{TRANSACTION_SERVICE_URL}{TRANSACTION_SEND_PATH}",
            json={
                "sender": sender,
                "receiver": receiver,
                "amount": amount,
            },
        )
        assert resp.status_code == 200, (
            f"Failed to send transaction: {resp.text}"
        )
        assert resp.json()["status"] == "pending"

    def _wait_for_tx_in_pool(
        self,
        client: httpx.Client,
        sender: str,
        receiver: str,
        amount: float,
    ) -> None:
        deadline = time.time() + POLL_TIMEOUT_S
        tx_in_pool = False
        while time.time() < deadline:
            resp = client.get(
                f"{TRANSACTION_SERVICE_URL}"
                f"{TRANSACTION_PENDING_PATH}"
            )
            assert resp.status_code == 200, (
                f"Failed to get pending txs: {resp.text}"
            )
            pending_txs = resp.json()["transactions"]
            matching = [
                tx
                for tx in pending_txs
                if tx["sender"] == sender
                and tx["receiver"] == receiver
                and tx["amount"] == amount
            ]
            if len(matching) == 1:
                tx_in_pool = True
                break
            time.sleep(POLL_INTERVAL_S)

        assert tx_in_pool, (
            f"Transaction from {sender} to {receiver} "
            "did not appear in pending pool within timeout"
        )

    def _trigger_mining(
        self, client: httpx.Client
    ) -> dict:
        resp = client.post(
            f"{MINER_SERVICE_URL}{MINE_PATH}",
            timeout=120.0,
        )
        assert resp.status_code == 200, (
            f"Mining failed: {resp.text}"
        )
        mine_result = resp.json()
        assert mine_result["status"] == "success", (
            f"Mining did not succeed: {mine_result}"
        )
        return mine_result

    def _verify_blockchain_grew(
        self,
        client: httpx.Client,
        expected_before: int,
    ) -> None:
        resp = client.get(
            f"{BLOCKCHAIN_SERVICE_URL}{BLOCKCHAIN_PATH}"
        )
        assert resp.status_code == 200
        chain_length_after = resp.json()["length"]
        assert chain_length_after == expected_before + 1, (
            f"Blockchain should grow by 1 block: "
            f"before={expected_before}, "
            f"after={chain_length_after}"
        )

    def _verify_tx_no_longer_pending(
        self,
        client: httpx.Client,
        sender: str,
        receiver: str,
        amount: float,
    ) -> None:
        resp = client.get(
            f"{TRANSACTION_SERVICE_URL}"
            f"{TRANSACTION_PENDING_PATH}"
        )
        assert resp.status_code == 200
        pending_after = resp.json()["transactions"]
        still_pending = [
            tx
            for tx in pending_after
            if tx["sender"] == sender
            and tx["receiver"] == receiver
            and tx["amount"] == amount
        ]
        assert len(still_pending) == 0, (
            "Our transaction should no longer be in the "
            "pending pool after mining"
        )

    def _verify_balances(
        self,
        client: httpx.Client,
        miner_address: str,
        wallet_a: str,
        wallet_b: str,
        tx_amount: float,
        miner_balance_before: float,
        balance_a_before: float,
        balance_b_before: float,
    ) -> None:
        miner_balance_after = self._get_balance(
            client, miner_address
        )
        miner_delta = miner_balance_after - miner_balance_before
        assert miner_delta == pytest.approx(MINING_REWARD), (
            f"Miner balance should increase by "
            f"{MINING_REWARD}, got delta {miner_delta}"
        )

        balance_a_after = self._get_balance(client, wallet_a)
        delta_a = balance_a_after - balance_a_before
        assert delta_a == pytest.approx(-tx_amount), (
            f"Wallet A balance should decrease by "
            f"{tx_amount}, got delta {delta_a}"
        )

        balance_b_after = self._get_balance(client, wallet_b)
        delta_b = balance_b_after - balance_b_before
        assert delta_b == pytest.approx(tx_amount), (
            f"Wallet B balance should increase by "
            f"{tx_amount}, got delta {delta_b}"
        )

    def test_full_flow(self):
        """
        Validate that all four microservices work together.

        Flow:
        1. Create two wallets via Wallet Service
        2. Submit a transaction between them
        3. Verify the transaction appears in the pending pool
        4. Trigger mining via Miner Service
        5. Confirm blockchain grew and pending pool cleared
        6. Validate balances reflect the transaction and
           mining rewards
        """
        with httpx.Client(timeout=30.0) as client:
            # Step 1: Create two wallets
            wallet_a, wallet_b = self._create_wallets(client)

            # Step 2: Submit a transaction
            tx_amount = 10.0
            self._submit_transaction(
                client, wallet_a, wallet_b, tx_amount
            )

            # Step 3: Verify tx appears in pending pool
            self._wait_for_tx_in_pool(
                client, wallet_a, wallet_b, tx_amount
            )

            # Record blockchain length before mining
            resp = client.get(
                f"{BLOCKCHAIN_SERVICE_URL}{BLOCKCHAIN_PATH}"
            )
            assert resp.status_code == 200
            chain_length_before = resp.json()["length"]

            # Get miner address from the running service
            resp = client.get(
                f"{MINER_SERVICE_URL}{MINER_STATS_PATH}"
            )
            assert resp.status_code == 200, (
                f"Failed to get miner stats: {resp.text}"
            )
            miner_address = resp.json()["miner_address"]

            # Record balances before mining
            miner_bal_before = self._get_balance(
                client, miner_address
            )
            bal_a_before = self._get_balance(client, wallet_a)
            bal_b_before = self._get_balance(client, wallet_b)

            # Step 4: Trigger mining
            self._trigger_mining(client)

            # Step 5: Confirm blockchain grew and pool cleared
            self._verify_blockchain_grew(
                client, chain_length_before
            )
            self._verify_tx_no_longer_pending(
                client, wallet_a, wallet_b, tx_amount
            )

            # Step 6: Validate balances
            self._verify_balances(
                client,
                miner_address,
                wallet_a,
                wallet_b,
                tx_amount,
                miner_bal_before,
                bal_a_before,
                bal_b_before,
            )

            # Blockchain integrity check
            resp = client.get(
                f"{BLOCKCHAIN_SERVICE_URL}"
                f"{BLOCKCHAIN_VALIDATE_PATH}"
            )
            assert resp.status_code == 200
            assert resp.json()["valid"] is True, (
                "Blockchain should be valid after mining"
            )
