import asyncio
from collections import defaultdict

from app.features.auth.registration_v2 import RegistrationFlow


class FakeUsers:
    def __init__(self, discord_docs=(), linked_docs=()):
        self.discord_docs = list(discord_docs)
        self.linked_docs = list(linked_docs)

    async def find_users_by_discord_id(self, discord_id: str):
        return list(self.discord_docs)

    async def find_users_by_linked_account_id(self, linked_account_id: str):
        return list(self.linked_docs)


class NoSuspensions:
    async def find(self, flt):
        for doc in ():
            yield doc


def lookup_flow(users):
    client = defaultdict(lambda: defaultdict(NoSuspensions))
    flow = RegistrationFlow(client)
    flow._users = users
    return flow


def test_a_discord_lookup_renders_older_epic_and_xbox_records():
    users = FakeUsers(
        discord_docs=[
            {
                "discord_id": "111111111111111111",
                "discord_username": "primary_test_user",
                "display_name": "Primary Test",
                "linked_platform": "epic",
                "linked_account_id": "epic-test-001",
                "registrations": {"civ6": {"method": "admin_staff_attested"}},
            },
            {
                "discord_id": "111111111111111111",
                "linked_platform": "xbox",
                "linked_account_id": "xb-1",
            },
        ]
    )
    records = asyncio.run(lookup_flow(users).by_discord("111111111111111111")).records
    assert [r.linked_platform.value for r in records] == ["epic", "xbox"]
    assert records[0].discord_username == "primary_test_user"
    assert records[0].registrations[0].method == "admin_staff_attested"


def test_a_linked_lookup_reads_steam_ids_older_writers_stored():
    users = FakeUsers(
        linked_docs=[
            {
                "discord_id": "111111111111111111",
                "user_name": "old_name",
                "steam_id": "76561190000000001",
                "steam_name": "Primary Steam",
            },
            {
                "discord_id": "222222222222222222",
                "linked_platform": "steam",
                "linked_account_id": "76561190000000001",
            },
        ]
    )
    records = asyncio.run(lookup_flow(users).by_linked("76561190000000001")).records
    first = records[0]
    assert (first.discord_username, first.linked_platform.value) == (
        "old_name",
        "steam",
    )
    assert (first.linked_account_id, first.linked_account_name) == (
        "76561190000000001",
        "Primary Steam",
    )
    assert [r.discord_id for r in records] == [
        "111111111111111111",
        "222222222222222222",
    ]
