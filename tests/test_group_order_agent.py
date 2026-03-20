import unittest
from decimal import Decimal

from group_order_agent import AgentConfig, GroupOrderAgent, Ledger, OrderInput, OrderItem


class GroupOrderAgentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger()
        for uid in ["u1", "u2"]:
            self.ledger.credit_wallet(uid, Decimal("300"))
        self.agent = GroupOrderAgent(
            ledger=self.ledger,
            config=AgentConfig(
                group_window_minutes=10,
                service_fee_rate=Decimal("0.03"),
                max_single_order_amount=Decimal("400"),
                nearby_address_meters=200,
                fuzzy_merchant_distance_meters=200,
                default_tolerance_minutes=5,
            ),
            store_path=":memory:",
        )

    def test_merge_checkout_and_delivery_waiver(self) -> None:
        o1 = self.agent.submit_order(
            OrderInput(
                user_id="u1",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towera",
                items=[OrderItem(name="a", unit_price=Decimal("35"), quantity=1)],
                idempotency_key="k1",
            )
        )
        o2 = self.agent.submit_order(
            OrderInput(
                user_id="u2",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towerb",
                items=[OrderItem(name="b", unit_price=Decimal("20"), quantity=1)],
                idempotency_key="k2",
            )
        )
        self.assertEqual(o1.room_id, o2.room_id)
        settlement = self.agent.run_checkout(o1.room_id)
        self.assertEqual(settlement.total_delivery, Decimal("0.00"))
        self.assertTrue(settlement.total_discount >= Decimal("5.00"))
        self.assertEqual(settlement.platform_revenue, Decimal("1.00"))
        self.assertTrue(settlement.total_payable > Decimal("0"))
        self.assertEqual(self.agent.rooms[o1.room_id].status, "PAID")
        self.assertTrue(self.ledger.account_balance("merchant:r_hotpot_001") > Decimal("0"))
        self.assertTrue(self.ledger.account_balance("platform:revenue") > Decimal("0"))

    def test_precise_matching_option(self) -> None:
        self.agent.submit_order(
            OrderInput(
                user_id="u2",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towerb",
                items=[OrderItem(name="b", unit_price=Decimal("25"), quantity=1)],
                idempotency_key="k-existing",
            )
        )
        order, matching = self.agent.submit_order_with_matching(
            OrderInput(
                user_id="u1",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towera",
                items=[OrderItem(name="a", unit_price=Decimal("30"), quantity=1)],
                idempotency_key="k-new",
            ),
            tolerance_minutes=5,
        )
        self.assertIsNotNone(order.order_id)
        self.assertTrue(len(matching["precise_options"]) > 0)

    def test_auto_checkout_when_free_delivery_hit(self) -> None:
        self.agent.submit_order(
            OrderInput(
                user_id="u2",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towerb",
                items=[OrderItem(name="b", unit_price=Decimal("20"), quantity=1)],
                idempotency_key="auto-1",
            )
        )
        order, matching = self.agent.submit_order_with_matching(
            OrderInput(
                user_id="u1",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towera",
                items=[OrderItem(name="a", unit_price=Decimal("30"), quantity=1)],
                idempotency_key="auto-2",
            ),
            tolerance_minutes=5,
        )
        self.assertTrue(matching["auto_settled"])
        self.assertEqual(self.agent.rooms[order.room_id].status, "PAID")

    def test_nearby_merchants_merge_across_far_addresses(self) -> None:
        o1 = self.agent.submit_order(
            OrderInput(
                user_id="u1",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towera",
                items=[OrderItem(name="a", unit_price=Decimal("28"), quantity=1)],
                idempotency_key="near-1",
            )
        )
        o2, matching = self.agent.submit_order_with_matching(
            OrderInput(
                user_id="u2",
                restaurant_id="r_spicy_003",
                address_tag="shanghai_jiaoda_dorma",
                items=[OrderItem(name="b", unit_price=Decimal("18"), quantity=1)],
                idempotency_key="near-2",
            )
        )
        self.assertEqual(o1.room_id, o2.room_id)
        self.assertTrue(matching["prompt_settle"])
        projected = self.agent.project_room_delivery(o1.room_id)
        self.assertTrue(projected["is_nearby_merchant_cluster"])
        self.assertTrue(projected["merged_delivery"] < projected["baseline_delivery"])

    def test_upsell_prompt_when_gap_within_ten_percent(self) -> None:
        _, matching = self.agent.submit_order_with_matching(
            OrderInput(
                user_id="u1",
                restaurant_id="r_hotpot_001",
                address_tag="shanghai_lujiazui_towera",
                items=[OrderItem(name="c", unit_price=Decimal("46"), quantity=1)],
                idempotency_key="upsell-1",
            )
        )
        self.assertTrue(len(matching["upsell_suggestions"]) > 0)
        names = [i["name"] for i in matching["upsell_suggestions"][0]["recommended_items"]]
        self.assertIn("猪肉饭", names)

    def test_jiaoda_addresses_are_nearby(self) -> None:
        d = self.agent._address_distance_m("shanghai_jiaoda_dorma", "shanghai_jiaoda_dormb")
        self.assertTrue(d < 120.0)

    def test_idempotency(self) -> None:
        o1 = self.agent.submit_order(
            OrderInput(
                user_id="u1",
                restaurant_id="r_salad_002",
                address_tag="shanghai_jiaoda_dorma",
                items=[OrderItem(name="x", unit_price=Decimal("20"), quantity=1)],
                idempotency_key="same-key",
            )
        )
        o2 = self.agent.submit_order(
            OrderInput(
                user_id="u1",
                restaurant_id="r_salad_002",
                address_tag="shanghai_jiaoda_dorma",
                items=[OrderItem(name="x", unit_price=Decimal("20"), quantity=1)],
                idempotency_key="same-key",
            )
        )
        self.assertEqual(o1.order_id, o2.order_id)

    def test_risk_reject_large_order(self) -> None:
        with self.assertRaises(ValueError):
            self.agent.submit_order(
                OrderInput(
                    user_id="u1",
                    restaurant_id="r_hotpot_001",
                    address_tag="shanghai_lujiazui_towera",
                    items=[OrderItem(name="big", unit_price=Decimal("401"), quantity=1)],
                    idempotency_key="risk-key",
                )
            )


if __name__ == "__main__":
    unittest.main()
