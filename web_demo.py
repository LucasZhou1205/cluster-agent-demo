from __future__ import annotations

import os
from decimal import Decimal

from flask import Flask, jsonify, render_template, request

from group_order_agent import AgentConfig, GroupOrderAgent, Ledger, OrderInput, OrderItem


USERS = ["u_alice", "u_bob", "u_cindy", "u_david"]
DEFAULT_ADDRESS = "shanghai_lujiazui_towera"


def _money(value: Decimal) -> str:
    return f"{value:.2f}"


def _serialize_room(agent: GroupOrderAgent, room_id: str) -> dict:
    room = agent.rooms[room_id]
    orders = [agent.orders[oid] for oid in room.order_ids]
    restaurants = sorted({o.restaurant_id for o in orders})
    return {
        "room_id": room.room_id,
        "restaurant_id": room.restaurant_id,
        "restaurants": restaurants,
        "address_tag": room.address_tag,
        "status": room.status,
        "open_at": room.open_at.isoformat(),
        "close_at": room.close_at.isoformat(),
        "orders": [
            {
                "order_id": o.order_id,
                "user_id": o.user_id,
                "food_subtotal": _money(o.food_subtotal),
                "items": [
                    {
                        "name": item.name,
                        "unit_price": _money(item.unit_price),
                        "quantity": item.quantity,
                        "subtotal": _money(item.subtotal),
                    }
                    for item in o.items
                ],
            }
            for o in orders
        ],
    }


def _serialize_state(agent: GroupOrderAgent, ledger: Ledger) -> dict:
    merchants = agent.list_merchants()
    return {
        "rooms": [_serialize_room(agent, rid) for rid in agent.rooms],
        "balances": {
            uid: _money(ledger.user_wallet_balance(uid)) for uid in USERS
        },
        "platform_balance": _money(ledger.account_balance("platform:revenue")),
        "merchant_balances": {
            r["id"]: _money(ledger.account_balance(f"merchant:{r['id']}"))
            for r in merchants
        },
        "addresses": agent.list_addresses(),
        "merchant_profiles": merchants,
        "menu_catalog": agent.list_menu_items(),
    }


def _agent_store_path() -> str:
    """Use /tmp on read-only serverless roots (e.g. Vercel when cwd is not writable)."""
    override = os.environ.get("AGENT_STORE_PATH")
    if override:
        return override
    tmp_root = os.environ.get("TMPDIR", "/tmp")
    probe = os.path.join(os.getcwd(), ".write_probe")
    try:
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return "cluster_pool.db"
    except OSError:
        return os.path.join(tmp_root, "cluster_pool.db")


def _create_runtime() -> tuple[GroupOrderAgent, Ledger]:
    ledger = Ledger()
    for user in USERS:
        ledger.credit_wallet(user, Decimal("300"))
    agent = GroupOrderAgent(
        ledger=ledger,
        config=AgentConfig(
            group_window_minutes=15,
            service_fee_rate=Decimal("0.03"),
            max_single_order_amount=Decimal("600"),
            nearby_address_meters=120,
            fuzzy_merchant_distance_meters=200,
            default_tolerance_minutes=5,
        ),
        store_path=_agent_store_path(),
    )
    return agent, ledger


_BASE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=os.path.join(_BASE, "templates"))
app.config["JSON_AS_ASCII"] = False
RUNTIME_AGENT, RUNTIME_LEDGER = _create_runtime()


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/orders")
def orders_page():
    return render_template("index.html")


@app.get("/api/bootstrap")
def bootstrap():
    return jsonify(
        {
            "users": USERS,
            "restaurants": RUNTIME_AGENT.list_merchants(),
            "addresses": RUNTIME_AGENT.list_addresses(),
            "menu_catalog": RUNTIME_AGENT.list_menu_items(),
            "default_address": DEFAULT_ADDRESS,
            "state": _serialize_state(RUNTIME_AGENT, RUNTIME_LEDGER),
        }
    )


@app.post("/api/reset")
def reset():
    global RUNTIME_AGENT, RUNTIME_LEDGER
    RUNTIME_AGENT, RUNTIME_LEDGER = _create_runtime()
    return jsonify({"ok": True, "state": _serialize_state(RUNTIME_AGENT, RUNTIME_LEDGER)})


@app.get("/api/state")
def state():
    return jsonify(_serialize_state(RUNTIME_AGENT, RUNTIME_LEDGER))


@app.post("/api/order")
def create_order():
    payload = request.get_json(force=True)
    order, match_result = RUNTIME_AGENT.submit_order_with_matching(
        OrderInput(
            user_id=payload["user_id"],
            restaurant_id=payload["restaurant_id"],
            address_tag=payload.get("address_tag", DEFAULT_ADDRESS),
            items=[
                OrderItem(
                    name=payload["item_name"],
                    unit_price=Decimal(str(payload["unit_price"])),
                    quantity=int(payload["quantity"]),
                )
            ],
            idempotency_key=payload["idempotency_key"],
        ),
        tolerance_minutes=int(payload.get("tolerance_minutes", 5)),
    )
    return jsonify(
        {
            "ok": True,
            "order_id": order.order_id,
            "room_id": order.room_id,
            "matching": match_result,
            "state": _serialize_state(RUNTIME_AGENT, RUNTIME_LEDGER),
        }
    )


@app.post("/api/checkout/<room_id>")
def checkout(room_id: str):
    settlement = RUNTIME_AGENT.run_checkout(room_id)
    return jsonify(
        {
            "ok": True,
            "settlement": {
                "room_id": settlement.room_id,
                "restaurant_id": settlement.restaurant_id,
                "total_food": _money(settlement.total_food),
                "total_discount": _money(settlement.total_discount),
                "baseline_delivery": _money(settlement.baseline_delivery),
                "saved_delivery": _money(settlement.saved_delivery),
                "total_delivery": _money(settlement.total_delivery),
                "total_service_fee": _money(settlement.total_service_fee),
                "total_payable": _money(settlement.total_payable),
                "platform_revenue": _money(settlement.platform_revenue),
                "user_payables": {k: _money(v) for k, v in settlement.user_payables.items()},
                "merchant_receivables": {k: _money(v) for k, v in settlement.merchant_receivables.items()},
            },
            "state": _serialize_state(RUNTIME_AGENT, RUNTIME_LEDGER),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
