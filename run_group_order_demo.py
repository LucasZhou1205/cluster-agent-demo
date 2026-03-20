from decimal import Decimal

from group_order_agent import AgentConfig, GroupOrderAgent, Ledger, OrderInput, OrderItem


def main() -> None:
    ledger = Ledger("group_order_ledger.db")
    for user in ["u_alice", "u_bob", "u_cindy"]:
        ledger.credit_wallet(user, Decimal("200"))

    agent = GroupOrderAgent(
        ledger=ledger,
        config=AgentConfig(
            group_window_minutes=15,
            service_fee_rate=Decimal("0.03"),
            max_single_order_amount=Decimal("600"),
            nearby_address_meters=150,
            fuzzy_merchant_distance_meters=200,
            default_tolerance_minutes=5,
        ),
    )

    o1 = agent.submit_order(
        OrderInput(
            user_id="u_alice",
            restaurant_id="r_hotpot_001",
            address_tag="shanghai_lujiazui_towera",
            items=[OrderItem(name="牛肉饭", unit_price=Decimal("32"), quantity=1)],
        )
    )
    o2 = agent.submit_order(
        OrderInput(
            user_id="u_bob",
            restaurant_id="r_hotpot_001",
            address_tag="shanghai_lujiazui_towerb",
            items=[
                OrderItem(name="黄焖鸡", unit_price=Decimal("26"), quantity=2),
                OrderItem(name="可乐", unit_price=Decimal("5"), quantity=1),
            ],
        )
    )
    o3 = agent.submit_order(
        OrderInput(
            user_id="u_cindy",
            restaurant_id="r_hotpot_001",
            address_tag="shanghai_lujiazui_towera",
            items=[OrderItem(name="沙拉", unit_price=Decimal("24"), quantity=1)],
        )
    )

    room_id = o1.room_id
    assert o2.room_id == room_id and o3.room_id == room_id
    settlement = agent.run_checkout(room_id)

    print("room_id:", settlement.room_id)
    print("restaurant_id:", settlement.restaurant_id)
    print("total_food:", settlement.total_food)
    print("total_discount:", settlement.total_discount)
    print("total_delivery:", settlement.total_delivery)
    print("total_payable:", settlement.total_payable)
    print("user_payables:", settlement.user_payables)
    print("merchant_balance:", ledger.account_balance("merchant:r_hotpot_001"))
    print("platform_balance:", ledger.account_balance("platform:revenue"))
    print("alice_wallet:", ledger.user_wallet_balance("u_alice"))
    print("bob_wallet:", ledger.user_wallet_balance("u_bob"))
    print("cindy_wallet:", ledger.user_wallet_balance("u_cindy"))


if __name__ == "__main__":
    main()
