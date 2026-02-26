import os
import time

import httpx
import pytest

from shared.constants import MINING_REWARD

# API endpoint paths
WALLET_CREATE_PATH = "/wallet/create"
TRANSACTION_SEND_PATH = "/transaction/send"
TRANSACTION_PENDING_PATH = "/transaction/pending"
BLOCKCHAIN_PATH = "/blockchain"
BLOCKCHAIN_BALANCE_PATH = "/blockchain/balance"
BLOCKCHAIN_VALIDATE_PATH = "/blockchain/validate"
MINER_STATS_PATH = "/miner/stats"
MINE_PATH = "/mine"


@pytest.fixture(scope="session")
def wallet_service_url():
    return os.getenv(
        "WALLET_SERVICE_URL", "http://localhost:8000"
    )


@pytest.fixture(scope="session")
def transaction_service_url():
    return os.getenv(
        "TRANSACTION_SERVICE_URL", "http://localhost:8001"
    )


@pytest.fixture(scope="session")
def blockchain_service_url():
    return os.getenv(
        "BLOCKCHAIN_SERVICE_URL", "http://localhost:8002"
    )


@pytest.fixture(scope="session")
def miner_service_url():
    return os.getenv(
        "MINER_SERVICE_URL", "http://localhost:8003"
    )


@pytest.fixture(scope="session")
def poll_timeout_s():
    return 10


@pytest.fixture(scope="session")
def poll_interval_s():
    return 0.5


@pytest.mark.integration
class TestEndToEndHappyPath:
    """E2E integration test: Wallet -> Transaction -> Miner -> Blockchain"""

    def _get_balance(
        self,
        client: httpx.Client,
        blockchain_url: str,
        address: str,
    ) -> float:
        resp = client.get(
            f"{blockchain_url}{BLOCKCHAIN_BALANCE_PATH}"
            f"/{address}"
        )
        resp.raise_for_status()
        data = resp.json()
        assert "balance" in data, (
            "Response from balance endpoint is missing "
            f"'balance' key: {data}"
        )
        return data["balance"]

    def _create_wallets(
        self,
        client: httpx.Client,
        wallet_url: str,
    ) -> tuple[str, str]:
        resp = client.post(
            f"{wallet_url}{WALLET_CREATE_PATH}"
        )
        resp.raise_for_status()
        data = resp.json()
        assert "address" in data, (
            f"Missing 'address' in response: {data}"
        )
        wallet_a = data["address"]

        resp = client.post(
            f"{wallet_url}{WALLET_CREATE_PATH}"
        )
        resp.raise_for_status()
        data = resp.json()
        assert "address" in data, (
            f"Missing 'address' in response: {data}"
        )
        wallet_b = data["address"]

        assert wallet_a != wallet_b, (
            "Wallet addresses must be unique"
        )
        return wallet_a, wallet_b

    def _submit_transaction(
        self,
        client: httpx.Client,
        transaction_url: str,
        sender: str,
        receiver: str,
        amount: float,
    ) -> None:
        resp = client.post(
            f"{transaction_url}{TRANSACTION_SEND_PATH}",
            json={
                "sender": sender,
                "receiver": receiver,
                "amount": amount,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        assert data.get("status") == "pending", (
            f"Expected status 'pending', got: {data}"
        )

    def _wait_for_tx_in_pool(
        self,
        client: httpx.Client,
        transaction_url: str,
        sender: str,
        receiver: str,
        amount: float,
        timeout_s: float,
        interval_s: float,
    ) -> None:
        deadline = time.time() + timeout_s
        tx_in_pool = False
        while time.time() < deadline:
            resp = client.get(
                f"{transaction_url}"
                f"{TRANSACTION_PENDING_PATH}"
            )
            resp.raise_for_status()
            data = resp.json()
            assert "transactions" in data, (
                "Missing 'transactions' in pending "
                f"response: {data}"
            )
            matching = [
                tx
                for tx in data["transactions"]
                if tx["sender"] == sender
                and tx["receiver"] == receiver
                and tx["amount"] == amount
            ]
            if len(matching) == 1:
                tx_in_pool = True
                break
            time.sleep(interval_s)

        assert tx_in_pool, (
            f"Transaction from {sender} to {receiver} "
            "did not appear in pending pool within timeout"
        )

    def _trigger_mining(
        self,
        client: httpx.Client,
        miner_url: str,
    ) -> dict:
        resp = client.post(
            f"{miner_url}{MINE_PATH}",
            timeout=120.0,
        )
        resp.raise_for_status()
        mine_result = resp.json()
        assert mine_result.get("status") == "success", (
            f"Mining did not succeed: {mine_result}"
        )
        return mine_result

    def _verify_blockchain_grew(
        self,
        client: httpx.Client,
        blockchain_url: str,
        expected_before: int,
    ) -> None:
        resp = client.get(
            f"{blockchain_url}{BLOCKCHAIN_PATH}"
        )
        resp.raise_for_status()
        data = resp.json()
        assert "length" in data, (
            f"Missing 'length' in blockchain response: "
            f"{data}"
        )
        chain_length_after = data["length"]
        assert chain_length_after == expected_before + 1, (
            f"Blockchain should grow by 1 block: "
            f"before={expected_before}, "
            f"after={chain_length_after}"
        )

    def _verify_tx_no_longer_pending(
        self,
        client: httpx.Client,
        transaction_url: str,
        sender: str,
        receiver: str,
        amount: float,
    ) -> None:
        resp = client.get(
            f"{transaction_url}"
            f"{TRANSACTION_PENDING_PATH}"
        )
        resp.raise_for_status()
        data = resp.json()
        assert "transactions" in data, (
            "Missing 'transactions' in pending "
            f"response: {data}"
        )
        still_pending = [
            tx
            for tx in data["transactions"]
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
        blockchain_url: str,
        miner_address: str,
        wallet_a: str,
        wallet_b: str,
        tx_amount: float,
        miner_balance_before: float,
        balance_a_before: float,
        balance_b_before: float,
    ) -> None:
        miner_balance_after = self._get_balance(
            client, blockchain_url, miner_address
        )
        miner_delta = (
            miner_balance_after - miner_balance_before
        )
        assert miner_delta == pytest.approx(MINING_REWARD), (
            f"Miner balance should increase by "
            f"{MINING_REWARD}, got delta {miner_delta}"
        )

        balance_a_after = self._get_balance(
            client, blockchain_url, wallet_a
        )
        delta_a = balance_a_after - balance_a_before
        assert delta_a == pytest.approx(-tx_amount), (
            f"Wallet A balance should decrease by "
            f"{tx_amount}, got delta {delta_a}"
        )

        balance_b_after = self._get_balance(
            client, blockchain_url, wallet_b
        )
        delta_b = balance_b_after - balance_b_before
        assert delta_b == pytest.approx(tx_amount), (
            f"Wallet B balance should increase by "
            f"{tx_amount}, got delta {delta_b}"
        )

    def test_full_flow(
        self,
        wallet_service_url,
        transaction_service_url,
        blockchain_service_url,
        miner_service_url,
        poll_timeout_s,
        poll_interval_s,
    ):
        """
        Validate that all four microservices work together.

        Flow:
        1. Create two wallets via Wallet Service
        2. Submit a transaction between them
        3. Verify the transaction appears in pending pool
        4. Trigger mining via Miner Service
        5. Confirm blockchain grew and pending pool cleared
        6. Validate balances reflect the transaction and
           mining rewards
        """
        with httpx.Client(timeout=30.0) as client:
            # Step 1: Create two wallets
            wallet_a, wallet_b = self._create_wallets(
                client, wallet_service_url
            )

            # Step 2: Submit a transaction
            tx_amount = 10.0
            self._submit_transaction(
                client,
                transaction_service_url,
                wallet_a,
                wallet_b,
                tx_amount,
            )

            # Step 3: Verify tx appears in pending pool
            self._wait_for_tx_in_pool(
                client,
                transaction_service_url,
                wallet_a,
                wallet_b,
                tx_amount,
                poll_timeout_s,
                poll_interval_s,
            )

            # Record blockchain length before mining
            resp = client.get(
                f"{blockchain_service_url}{BLOCKCHAIN_PATH}"
            )
            resp.raise_for_status()
            data = resp.json()
            assert "length" in data, (
                f"Missing 'length' in response: {data}"
            )
            chain_length_before = data["length"]

            # Get miner address from the running service
            resp = client.get(
                f"{miner_service_url}{MINER_STATS_PATH}"
            )
            resp.raise_for_status()
            data = resp.json()
            assert "miner_address" in data, (
                f"Missing 'miner_address' in stats: {data}"
            )
            miner_address = data["miner_address"]

            # Record balances before mining
            miner_bal_before = self._get_balance(
                client, blockchain_service_url, miner_address
            )
            bal_a_before = self._get_balance(
                client, blockchain_service_url, wallet_a
            )
            bal_b_before = self._get_balance(
                client, blockchain_service_url, wallet_b
            )

            # Step 4: Trigger mining
            mine_result = self._trigger_mining(
                client, miner_service_url
            )

            # Verify block index matches expectation
            if "block_index" in mine_result:
                assert (
                    mine_result["block_index"]
                    == chain_length_before + 1
                ), (
                    f"Expected block index "
                    f"{chain_length_before + 1}, "
                    f"got {mine_result['block_index']}"
                )

            # Step 5: Confirm blockchain grew & pool cleared
            self._verify_blockchain_grew(
                client,
                blockchain_service_url,
                chain_length_before,
            )
            self._verify_tx_no_longer_pending(
                client,
                transaction_service_url,
                wallet_a,
                wallet_b,
                tx_amount,
            )

            # Step 6: Validate balances
            self._verify_balances(
                client,
                blockchain_service_url,
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
                f"{blockchain_service_url}"
                f"{BLOCKCHAIN_VALIDATE_PATH}"
            )
            resp.raise_for_status()
            data = resp.json()
            assert data.get("valid") is True, (
                "Blockchain should be valid after mining"
            )
