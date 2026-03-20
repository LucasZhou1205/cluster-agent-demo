from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from .models import money


class Ledger:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ledger_entries (
                id TEXT PRIMARY KEY,
                tx_id TEXT NOT NULL,
                account TEXT NOT NULL,
                amount TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY
            )
            """
        )
        self.conn.commit()

    def ensure_user(self, user_id: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,))
        self.conn.commit()

    def account_balance(self, account: str) -> Decimal:
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) FROM ledger_entries WHERE account = ?",
            (account,),
        )
        row = cur.fetchone()
        return money(row[0] if row else 0)

    def user_wallet_balance(self, user_id: str) -> Decimal:
        return self.account_balance(f"wallet:{user_id}")

    def user_hold_balance(self, user_id: str) -> Decimal:
        return self.account_balance(f"hold:{user_id}")

    def credit_wallet(self, user_id: str, amount: Decimal) -> str:
        self.ensure_user(user_id)
        return self._post([("wallet:" + user_id, money(amount))])

    def hold(self, user_id: str, amount: Decimal) -> str:
        self.ensure_user(user_id)
        amount = money(amount)
        if self.user_wallet_balance(user_id) < amount:
            raise ValueError(f"用户 {user_id} 余额不足")
        return self._post(
            [
                ("wallet:" + user_id, money(-amount)),
                ("hold:" + user_id, amount),
            ]
        )

    def release_hold(self, user_id: str, amount: Decimal) -> str:
        amount = money(amount)
        if self.user_hold_balance(user_id) < amount:
            raise ValueError(f"用户 {user_id} 冻结金额不足")
        return self._post(
            [
                ("hold:" + user_id, money(-amount)),
                ("wallet:" + user_id, amount),
            ]
        )

    def capture_from_hold(self, user_id: str, merchant_id: str, merchant_amount: Decimal, platform_amount: Decimal) -> str:
        merchant_amount = money(merchant_amount)
        platform_amount = money(platform_amount)
        total = money(merchant_amount + platform_amount)
        if self.user_hold_balance(user_id) < total:
            raise ValueError(f"用户 {user_id} 冻结金额不足以扣款")
        return self._post(
            [
                ("hold:" + user_id, money(-total)),
                ("merchant:" + merchant_id, merchant_amount),
                ("platform:revenue", platform_amount),
            ]
        )

    def capture_from_hold_split(self, user_id: str, merchant_amounts: dict[str, Decimal], platform_amount: Decimal) -> str:
        normalized = {mid: money(v) for mid, v in merchant_amounts.items()}
        platform_amount = money(platform_amount)
        total = money(sum(normalized.values(), start=money(0)) + platform_amount)
        if self.user_hold_balance(user_id) < total:
            raise ValueError(f"用户 {user_id} 冻结金额不足以扣款")
        entries = [("hold:" + user_id, money(-total))]
        for merchant_id, amt in normalized.items():
            entries.append(("merchant:" + merchant_id, amt))
        entries.append(("platform:revenue", platform_amount))
        return self._post(entries)

    def _post(self, entries: list[tuple[str, Decimal]]) -> str:
        tx_id = str(uuid4())
        now = datetime.now(UTC).isoformat()
        for account, amount in entries:
            self.conn.execute(
                "INSERT INTO ledger_entries(id, tx_id, account, amount, created_at) VALUES(?, ?, ?, ?, ?)",
                (str(uuid4()), tx_id, account, str(money(amount)), now),
            )
        self.conn.commit()
        return tx_id
