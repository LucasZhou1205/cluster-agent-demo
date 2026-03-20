from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import math
import sqlite3
from uuid import uuid4

from .models import GroupRoom, Order, OrderInput, RoomSettlement, money
from .payments import Ledger


@dataclass
class AgentConfig:
    group_window_minutes: int = 12
    service_fee_rate: Decimal = money("0.03")
    max_single_order_amount: Decimal = money("500")
    nearby_address_meters: float = 120.0
    fuzzy_merchant_distance_meters: float = 200.0
    default_tolerance_minutes: int = 5


class GroupOrderAgent:
    def __init__(self, ledger: Ledger, config: AgentConfig | None = None, store_path: str = "cluster_pool.db") -> None:
        self.ledger = ledger
        self.config = config or AgentConfig()
        self.rooms: dict[str, GroupRoom] = {}
        self.orders: dict[str, Order] = {}
        self.idempotency_map: dict[str, str] = {}
        self.store = sqlite3.connect(store_path, check_same_thread=False)
        self._init_store()
        self._seed_profiles()

    def submit_order(self, payload: OrderInput) -> Order:
        if payload.idempotency_key in self.idempotency_map:
            return self.orders[self.idempotency_map[payload.idempotency_key]]
        if payload.food_subtotal > self.config.max_single_order_amount:
            raise ValueError("触发风控：单笔订单金额超限")
        room = self._find_or_create_room(payload)
        order = Order(
            order_id=str(uuid4()),
            room_id=room.room_id,
            user_id=payload.user_id,
            restaurant_id=payload.restaurant_id,
            address_tag=payload.address_tag,
            items=payload.items,
            created_at=payload.created_at,
            food_subtotal=payload.food_subtotal,
        )
        room.order_ids.append(order.order_id)
        self.orders[order.order_id] = order
        self.idempotency_map[payload.idempotency_key] = order.order_id
        self._save_pool_order(order)
        return order

    def submit_order_with_matching(self, payload: OrderInput, tolerance_minutes: int | None = None) -> tuple[Order, dict]:
        order = self.submit_order(payload)
        options = self.scan_match_options(order.order_id, tolerance_minutes=tolerance_minutes)
        auto_settled = False
        prompt_settle = False
        projected = self.project_room_delivery(order.room_id)
        if projected["merged_delivery"] == money(0) and self.rooms[order.room_id].status == "OPEN":
            self.run_checkout(order.room_id)
            auto_settled = True
        elif projected["is_nearby_merchant_cluster"] and projected["merged_delivery"] > money(0):
            prompt_settle = True
        options["auto_settled"] = auto_settled
        options["prompt_settle"] = prompt_settle
        options["delivery_projection"] = {
            "baseline_delivery": f"{projected['baseline_delivery']:.2f}",
            "merged_delivery": f"{projected['merged_delivery']:.2f}",
            "saved_delivery": f"{projected['saved_delivery']:.2f}",
            "is_nearby_merchant_cluster": projected["is_nearby_merchant_cluster"],
        }
        return order, options

    def list_addresses(self) -> list[dict]:
        cur = self.store.execute("SELECT tag, lat, lon FROM addresses ORDER BY tag")
        return [{"tag": row[0], "lat": row[1], "lon": row[2]} for row in cur.fetchall()]

    def list_merchants(self) -> list[dict]:
        cur = self.store.execute(
            "SELECT restaurant_id, name, category, lat, lon, base_delivery_fee, free_threshold, max_waiver FROM merchant_policies ORDER BY restaurant_id"
        )
        rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "lat": row[3],
                "lon": row[4],
                "base_delivery_fee": f"{Decimal(str(row[5])):.2f}",
                "free_threshold": f"{Decimal(str(row[6])):.2f}",
                "max_waiver": f"{Decimal(str(row[7])):.2f}",
            }
            for row in rows
        ]

    def list_menu_items(self) -> dict[str, list[dict]]:
        cur = self.store.execute(
            "SELECT restaurant_id, item_name, price FROM menu_items ORDER BY restaurant_id, price, item_name"
        )
        out: dict[str, list[dict]] = {}
        for restaurant_id, item_name, price in cur.fetchall():
            out.setdefault(restaurant_id, []).append({"name": item_name, "price": f"{Decimal(str(price)):.2f}"})
        return out

    def scan_match_options(self, order_id: str, tolerance_minutes: int | None = None) -> dict:
        order = self.orders[order_id]
        tolerance = tolerance_minutes or self.config.default_tolerance_minutes
        self._close_expired_demands(tolerance)
        precise = self._scan_precise(order)
        fuzzy_geo = []
        fuzzy_brand = []
        if not precise:
            fuzzy_geo = self._scan_fuzzy_geo(order)
        if not precise and not fuzzy_geo:
            fuzzy_brand = self._scan_fuzzy_brand(order)
        wait_tip = self._scan_wait_tip(order)
        closed = False
        age_minutes = (datetime.now(UTC) - order.created_at).total_seconds() / 60
        if age_minutes >= tolerance and not precise and not fuzzy_geo and not fuzzy_brand:
            self._mark_pool_status(order.order_id, "CLOSED")
            closed = True
        return {
            "precise_options": precise,
            "fuzzy_geo_options": fuzzy_geo,
            "fuzzy_brand_options": fuzzy_brand,
            "wait_suggestion": wait_tip,
            "upsell_suggestions": self._build_upsell_suggestions(order.room_id),
            "request_closed": closed,
            "tolerance_minutes": tolerance,
        }

    def settle_room(self, room_id: str) -> RoomSettlement:
        room = self.rooms[room_id]
        room.status = "LOCKED"
        room_orders = [self.orders[oid] for oid in room.order_ids]
        total_food = money(sum((o.food_subtotal for o in room_orders), start=money(0)))
        projected = self.project_room_delivery(room_id)
        baseline_delivery = projected["baseline_delivery"]
        total_delivery = projected["merged_delivery"]
        saved_delivery = projected["saved_delivery"]
        platform_revenue = money(saved_delivery * Decimal("0.20"))
        total_discount = saved_delivery
        users = sorted({o.user_id for o in room_orders})
        user_food: dict[str, Decimal] = {u: money(0) for u in users}
        user_food_by_merchant: dict[str, dict[str, Decimal]] = {u: {} for u in users}
        merchant_food: dict[str, Decimal] = {}
        for o in room_orders:
            user_food[o.user_id] = money(user_food[o.user_id] + o.food_subtotal)
            user_food_by_merchant[o.user_id][o.restaurant_id] = money(
                user_food_by_merchant[o.user_id].get(o.restaurant_id, money(0)) + o.food_subtotal
            )
            merchant_food[o.restaurant_id] = money(merchant_food.get(o.restaurant_id, money(0)) + o.food_subtotal)

        user_payables: dict[str, Decimal] = {}
        user_platform_allocations: dict[str, Decimal] = {}
        user_merchant_allocations: dict[str, dict[str, Decimal]] = {}
        merchant_receivables: dict[str, Decimal] = {m: money(0) for m in merchant_food}

        merchant_delivery_weights = self._merchant_delivery_weights(room_orders)
        for user_id in users:
            food_amount = user_food[user_id]
            user_delivery = money(total_delivery / len(users))
            payable = money(food_amount + user_delivery)
            user_payables[user_id] = payable
            if total_food == money(0):
                user_platform_allocations[user_id] = money(0)
            else:
                user_platform_allocations[user_id] = money(platform_revenue * (food_amount / total_food))
            allocations_pre_platform: dict[str, Decimal] = {}
            for merchant_id, m_food in user_food_by_merchant[user_id].items():
                allocations_pre_platform[merchant_id] = money(m_food)
            for merchant_id, weight in merchant_delivery_weights.items():
                allocations_pre_platform[merchant_id] = money(
                    allocations_pre_platform.get(merchant_id, money(0)) + money(user_delivery * weight)
                )
            pre_total = money(sum(allocations_pre_platform.values(), start=money(0)))
            user_merchant_allocations[user_id] = {}
            for merchant_id, amt in allocations_pre_platform.items():
                if pre_total == money(0):
                    cut = money(0)
                else:
                    cut = money(user_platform_allocations[user_id] * (amt / pre_total))
                final_amt = money(amt - cut)
                user_merchant_allocations[user_id][merchant_id] = final_amt
                merchant_receivables[merchant_id] = money(merchant_receivables.get(merchant_id, money(0)) + final_amt)

        total_service_fee = money(0)
        total_payable = money(sum(user_payables.values(), start=money(0)))
        return RoomSettlement(
            room_id=room_id,
            restaurant_id=room.restaurant_id,
            total_food=total_food,
            total_discount=total_discount,
            total_delivery=total_delivery,
            total_service_fee=total_service_fee,
            total_payable=total_payable,
            baseline_delivery=baseline_delivery,
            saved_delivery=saved_delivery,
            platform_revenue=platform_revenue,
            user_payables=user_payables,
            merchant_receivables=merchant_receivables,
            user_merchant_allocations=user_merchant_allocations,
            user_platform_allocations=user_platform_allocations,
        )

    def authorize_room(self, settlement: RoomSettlement) -> dict[str, str]:
        txs: dict[str, str] = {}
        for user_id, due in settlement.user_payables.items():
            txs[user_id] = self.ledger.hold(user_id, due)
        return txs

    def capture_room(self, settlement: RoomSettlement) -> dict[str, str]:
        room = self.rooms[settlement.room_id]
        users = list(settlement.user_payables.keys())
        txs: dict[str, str] = {}
        for user_id in users:
            txs[user_id] = self.ledger.capture_from_hold_split(
                user_id=user_id,
                merchant_amounts=settlement.user_merchant_allocations[user_id],
                platform_amount=settlement.user_platform_allocations[user_id],
            )
        room.status = "PAID"
        self._mark_room_orders_status(room.order_ids, "PAID")
        return txs

    def run_checkout(self, room_id: str) -> RoomSettlement:
        settlement = self.settle_room(room_id)
        self.authorize_room(settlement)
        self.capture_room(settlement)
        return settlement

    def _find_or_create_room(self, payload: OrderInput) -> GroupRoom:
        for room in self.rooms.values():
            if room.status != "OPEN" or room.close_at < payload.created_at:
                continue
            restaurants = self._room_restaurants(room)
            if payload.restaurant_id in restaurants:
                if self._address_distance_m(room.address_tag, payload.address_tag) <= self.config.nearby_address_meters:
                    return room
            if any(self._merchant_distance_m(payload.restaurant_id, rid) <= self.config.fuzzy_merchant_distance_meters for rid in restaurants):
                return room
        room_id = str(uuid4())
        room = GroupRoom(
            room_id=room_id,
            restaurant_id=payload.restaurant_id,
            address_tag=payload.address_tag,
            open_at=payload.created_at,
            close_at=payload.created_at + timedelta(minutes=self.config.group_window_minutes),
        )
        self.rooms[room_id] = room
        return room

    def _init_store(self) -> None:
        self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS addresses (
                tag TEXT PRIMARY KEY,
                lat REAL NOT NULL,
                lon REAL NOT NULL
            )
            """
        )
        self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS merchant_policies (
                restaurant_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                base_delivery_fee REAL NOT NULL,
                free_threshold REAL NOT NULL,
                max_waiver REAL NOT NULL
            )
            """
        )
        self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS pool_orders (
                order_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                restaurant_id TEXT NOT NULL,
                address_tag TEXT NOT NULL,
                subtotal REAL NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
        self.store.execute(
            """
            CREATE TABLE IF NOT EXISTS menu_items (
                restaurant_id TEXT NOT NULL,
                item_name TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (restaurant_id, item_name)
            )
            """
        )
        self.store.commit()

    def _seed_profiles(self) -> None:
        addresses = [
            ("shanghai_lujiazui_towera", 31.241286, 121.501692),
            ("shanghai_lujiazui_towerb", 31.241992, 121.502403),
            ("shanghai_jiaoda_dorma", 31.201255, 121.430102),
            ("shanghai_jiaoda_dormb", 31.201530, 121.430420),
            ("shanghai_putuo_tower_a", 31.249300, 121.401600),
        ]
        for tag, lat, lon in addresses:
            self.store.execute(
                "INSERT OR IGNORE INTO addresses(tag, lat, lon) VALUES(?, ?, ?)",
                (tag, lat, lon),
            )
        merchants = [
            ("r_hotpot_001", "川味小馆", "川菜", 31.241350, 121.501500, 5.0, 50.0, 5.0),
            ("r_salad_002", "轻食能量碗", "轻食", 31.201700, 121.430900, 8.0, 80.0, 8.0),
            ("r_spicy_003", "蜀香便当", "川菜", 31.241880, 121.502120, 6.0, 48.0, 6.0),
        ]
        for row in merchants:
            self.store.execute(
                """
                INSERT OR REPLACE INTO merchant_policies
                (restaurant_id, name, category, lat, lon, base_delivery_fee, free_threshold, max_waiver)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        menu_rows = [
            ("r_hotpot_001", "牛肉饭", 28.0),
            ("r_hotpot_001", "鸡肉饭", 18.0),
            ("r_hotpot_001", "猪肉饭", 15.0),
            ("r_spicy_003", "牛肉饭", 28.0),
            ("r_spicy_003", "鸡肉饭", 18.0),
            ("r_spicy_003", "猪肉饭", 15.0),
            ("r_salad_002", "沙拉", 20.0),
            ("r_salad_002", "牛排", 45.0),
        ]
        for row in menu_rows:
            self.store.execute(
                "INSERT OR REPLACE INTO menu_items(restaurant_id, item_name, price) VALUES (?, ?, ?)",
                row,
            )
        self.store.commit()

    def _save_pool_order(self, order: Order) -> None:
        self.store.execute(
            """
            INSERT OR REPLACE INTO pool_orders(order_id, user_id, restaurant_id, address_tag, subtotal, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.order_id,
                order.user_id,
                order.restaurant_id,
                order.address_tag,
                float(order.food_subtotal),
                order.created_at.isoformat(),
                "OPEN",
            ),
        )
        self.store.commit()

    def _mark_pool_status(self, order_id: str, status: str) -> None:
        self.store.execute("UPDATE pool_orders SET status = ? WHERE order_id = ?", (status, order_id))
        self.store.commit()

    def _mark_room_orders_status(self, order_ids: list[str], status: str) -> None:
        for oid in order_ids:
            self.store.execute("UPDATE pool_orders SET status = ? WHERE order_id = ?", (status, oid))
        self.store.commit()

    def _close_expired_demands(self, tolerance_minutes: int) -> None:
        threshold = datetime.now(UTC) - timedelta(minutes=tolerance_minutes)
        self.store.execute(
            "UPDATE pool_orders SET status = 'CLOSED' WHERE status = 'OPEN' AND created_at < ?",
            (threshold.isoformat(),),
        )
        self.store.commit()

    def _merchant_policy(self, restaurant_id: str) -> dict:
        cur = self.store.execute(
            "SELECT name, category, lat, lon, base_delivery_fee, free_threshold, max_waiver FROM merchant_policies WHERE restaurant_id = ?",
            (restaurant_id,),
        )
        row = cur.fetchone()
        if not row:
            return {
                "name": restaurant_id,
                "category": "其他",
                "lat": 0.0,
                "lon": 0.0,
                "base_delivery_fee": 8.0,
                "free_threshold": 9999.0,
                "max_waiver": 0.0,
            }
        return {
            "name": row[0],
            "category": row[1],
            "lat": row[2],
            "lon": row[3],
            "base_delivery_fee": row[4],
            "free_threshold": row[5],
            "max_waiver": row[6],
        }

    def _delivery_fee(self, restaurant_id: str, subtotal: Decimal) -> Decimal:
        policy = self._merchant_policy(restaurant_id)
        base_fee = money(policy["base_delivery_fee"])
        threshold = money(policy["free_threshold"])
        waiver = money(policy["max_waiver"]) if subtotal >= threshold else money(0)
        fee = money(base_fee - waiver)
        if fee < money(0):
            return money(0)
        return fee

    def _address_distance_m(self, a_tag: str, b_tag: str) -> float:
        if a_tag == b_tag:
            return 0.0
        cur = self.store.execute("SELECT lat, lon FROM addresses WHERE tag = ?", (a_tag,))
        a = cur.fetchone()
        cur = self.store.execute("SELECT lat, lon FROM addresses WHERE tag = ?", (b_tag,))
        b = cur.fetchone()
        if not a or not b:
            return 999999.0
        return self._haversine_m(a[0], a[1], b[0], b[1])

    def _merchant_distance_m(self, a_restaurant: str, b_restaurant: str) -> float:
        a = self._merchant_policy(a_restaurant)
        b = self._merchant_policy(b_restaurant)
        return self._haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])

    def _haversine_m(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r = 6371000.0
        p1 = math.radians(lat1)
        p2 = math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        s = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.atan2(math.sqrt(s), math.sqrt(1 - s))

    def _open_pool_candidates(self, exclude_order_id: str) -> list[dict]:
        cur = self.store.execute(
            "SELECT order_id, user_id, restaurant_id, address_tag, subtotal, created_at FROM pool_orders WHERE status = 'OPEN' AND order_id != ?",
            (exclude_order_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "order_id": r[0],
                "user_id": r[1],
                "restaurant_id": r[2],
                "address_tag": r[3],
                "subtotal": money(r[4]),
                "created_at": datetime.fromisoformat(r[5]),
            }
            for r in rows
        ]

    def _scan_precise(self, order: Order) -> list[dict]:
        candidates = self._open_pool_candidates(order.order_id)
        results: list[dict] = []
        alone_delivery = self._delivery_fee(order.restaurant_id, order.food_subtotal)
        policy = self._merchant_policy(order.restaurant_id)
        for c in candidates:
            if c["restaurant_id"] != order.restaurant_id:
                continue
            distance = self._address_distance_m(order.address_tag, c["address_tag"])
            if distance > self.config.nearby_address_meters:
                continue
            merged_subtotal = money(order.food_subtotal + c["subtotal"])
            merged_delivery = self._delivery_fee(order.restaurant_id, merged_subtotal)
            merged_share = money(merged_delivery / Decimal("2"))
            saving = money(alone_delivery - merged_share)
            if saving <= money(0):
                continue
            results.append(
                {
                    "type": "精准匹配",
                    "level": "同楼同商家" if distance == 0 else "相近地址同商家",
                    "target_order_id": c["order_id"],
                    "distance_m": round(distance, 1),
                    "restaurant_name": policy["name"],
                    "before_delivery": f"{alone_delivery:.2f}",
                    "after_delivery_share": f"{merged_share:.2f}",
                    "saving_amount": f"{saving:.2f}",
                    "merged_subtotal": f"{merged_subtotal:.2f}",
                    "threshold": f"{money(policy['free_threshold']):.2f}",
                    "is_free_delivery_unlocked": merged_subtotal >= money(policy["free_threshold"]),
                }
            )
        results.sort(key=lambda x: Decimal(x["saving_amount"]), reverse=True)
        return results[:3]

    def _scan_fuzzy_geo(self, order: Order) -> list[dict]:
        candidates = self._open_pool_candidates(order.order_id)
        alone_delivery = self._delivery_fee(order.restaurant_id, order.food_subtotal)
        results: list[dict] = []
        for c in candidates:
            if c["restaurant_id"] == order.restaurant_id:
                continue
            merchant_distance = self._merchant_distance_m(order.restaurant_id, c["restaurant_id"])
            if merchant_distance > self.config.fuzzy_merchant_distance_meters:
                continue
            merged_alt_delivery = money((alone_delivery + self._delivery_fee(c["restaurant_id"], c["subtotal"])) * Decimal("0.5"))
            merged_alt_share = money(merged_alt_delivery / Decimal("2"))
            drop_rate = Decimal("0")
            if alone_delivery > money(0):
                drop_rate = (alone_delivery - merged_alt_share) / alone_delivery
            if drop_rate <= Decimal("0.5"):
                continue
            policy = self._merchant_policy(c["restaurant_id"])
            results.append(
                {
                    "type": "模糊协商-地理",
                    "target_order_id": c["order_id"],
                    "target_restaurant_id": c["restaurant_id"],
                    "target_restaurant_name": policy["name"],
                    "merchant_distance_m": round(merchant_distance, 1),
                    "before_delivery": f"{alone_delivery:.2f}",
                    "after_delivery_share": f"{merged_alt_share:.2f}",
                    "drop_rate": f"{(drop_rate * Decimal('100')).quantize(Decimal('0.1'))}%",
                }
            )
        return results[:2]

    def _scan_fuzzy_brand(self, order: Order) -> list[dict]:
        candidates = self._open_pool_candidates(order.order_id)
        current_policy = self._merchant_policy(order.restaurant_id)
        current_delivery = self._delivery_fee(order.restaurant_id, order.food_subtotal)
        current_total = money(order.food_subtotal + current_delivery)
        results: list[dict] = []
        for c in candidates:
            if c["restaurant_id"] == order.restaurant_id:
                continue
            target_policy = self._merchant_policy(c["restaurant_id"])
            if target_policy["category"] != current_policy["category"]:
                continue
            alt_delivery = self._delivery_fee(c["restaurant_id"], money(order.food_subtotal + c["subtotal"]))
            alt_share = money(alt_delivery / Decimal("2"))
            alt_total = money(order.food_subtotal + alt_share)
            if current_total <= money(0):
                continue
            saving_rate = (current_total - alt_total) / current_total
            if saving_rate <= Decimal("0.15"):
                continue
            results.append(
                {
                    "type": "模糊协商-品牌",
                    "target_order_id": c["order_id"],
                    "target_restaurant_id": c["restaurant_id"],
                    "target_restaurant_name": target_policy["name"],
                    "category": target_policy["category"],
                    "net_saving_rate": f"{(saving_rate * Decimal('100')).quantize(Decimal('0.1'))}%",
                    "before_total": f"{current_total:.2f}",
                    "after_total": f"{alt_total:.2f}",
                }
            )
        return results[:2]

    def _scan_wait_tip(self, order: Order) -> dict | None:
        policy = self._merchant_policy(order.restaurant_id)
        threshold = money(policy["free_threshold"])
        gap = money(threshold - order.food_subtotal)
        if gap <= money(0):
            return None
        if gap > money("20"):
            return None
        alone_delivery = self._delivery_fee(order.restaurant_id, order.food_subtotal)
        if alone_delivery <= money(0):
            return None
        projected_saving = alone_delivery
        return {
            "type": "时间建议",
            "suggest_wait_minutes": 5,
            "missing_amount_to_threshold": f"{gap:.2f}",
            "projected_delivery_saving": f"{projected_saving:.2f}",
            "message": "预测5分钟内有新订单可触发更优配送费减免，建议稍后结算",
        }

    def project_room_delivery(self, room_id: str) -> dict:
        room_orders = [self.orders[oid] for oid in self.rooms[room_id].order_ids]
        baseline_delivery = money(
            sum((self._delivery_fee(o.restaurant_id, o.food_subtotal) for o in room_orders), start=money(0))
        )
        restaurants = {o.restaurant_id for o in room_orders}
        is_same_merchant = len(restaurants) == 1
        if is_same_merchant:
            only = next(iter(restaurants))
            total_food = money(sum((o.food_subtotal for o in room_orders), start=money(0)))
            merged_delivery = self._delivery_fee(only, total_food)
            is_nearby_cluster = False
        else:
            all_near = self._all_merchants_near(restaurants)
            if all_near:
                merged_delivery = money(baseline_delivery * Decimal("0.50"))
                is_nearby_cluster = True
            else:
                merged_delivery = baseline_delivery
                is_nearby_cluster = False
        saved_delivery = money(baseline_delivery - merged_delivery)
        if saved_delivery < money(0):
            saved_delivery = money(0)
        return {
            "baseline_delivery": baseline_delivery,
            "merged_delivery": merged_delivery,
            "saved_delivery": saved_delivery,
            "is_nearby_merchant_cluster": is_nearby_cluster,
        }

    def _merchant_delivery_weights(self, room_orders: list[Order]) -> dict[str, Decimal]:
        by_merchant: dict[str, Decimal] = {}
        for o in room_orders:
            by_merchant[o.restaurant_id] = money(
                by_merchant.get(o.restaurant_id, money(0)) + self._delivery_fee(o.restaurant_id, o.food_subtotal)
            )
        total = money(sum(by_merchant.values(), start=money(0)))
        if total == money(0):
            count = len(by_merchant) or 1
            return {k: money(Decimal("1") / Decimal(str(count))) for k in by_merchant}
        return {k: money(v / total) for k, v in by_merchant.items()}

    def _all_merchants_near(self, restaurants: set[str]) -> bool:
        ids = list(restaurants)
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                if self._merchant_distance_m(ids[i], ids[j]) > self.config.fuzzy_merchant_distance_meters:
                    return False
        return True

    def _room_restaurants(self, room: GroupRoom) -> set[str]:
        return {self.orders[oid].restaurant_id for oid in room.order_ids if oid in self.orders}

    def _room_merchant_subtotals(self, room_id: str) -> dict[str, Decimal]:
        out: dict[str, Decimal] = {}
        for oid in self.rooms[room_id].order_ids:
            order = self.orders[oid]
            out[order.restaurant_id] = money(out.get(order.restaurant_id, money(0)) + order.food_subtotal)
        return out

    def _menu_for_merchant(self, restaurant_id: str) -> list[dict]:
        cur = self.store.execute(
            "SELECT item_name, price FROM menu_items WHERE restaurant_id = ? ORDER BY price, item_name",
            (restaurant_id,),
        )
        return [{"name": r[0], "price": money(r[1])} for r in cur.fetchall()]

    def _build_upsell_suggestions(self, room_id: str) -> list[dict]:
        subtotals = self._room_merchant_subtotals(room_id)
        suggestions: list[dict] = []
        for restaurant_id, subtotal in subtotals.items():
            policy = self._merchant_policy(restaurant_id)
            threshold = money(policy["free_threshold"])
            gap = money(threshold - subtotal)
            if gap <= money(0):
                continue
            if gap > money(threshold * Decimal("0.10")):
                continue
            menu = self._menu_for_merchant(restaurant_id)
            if not menu:
                continue
            ranked = sorted(menu, key=lambda x: (money(abs(x["price"] - gap)), x["price"]))
            picks = ranked[:2]
            suggestions.append(
                {
                    "restaurant_id": restaurant_id,
                    "restaurant_name": policy["name"],
                    "current_subtotal": f"{subtotal:.2f}",
                    "threshold": f"{threshold:.2f}",
                    "gap_to_free_delivery": f"{gap:.2f}",
                    "recommended_items": [{"name": p["name"], "price": f"{p['price']:.2f}"} for p in picks],
                    "message": "距离免配送费门槛仅差10%以内，是否加购推荐套餐立即享受免配送费？",
                }
            )
        return suggestions
