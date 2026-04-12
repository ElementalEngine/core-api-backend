import asyncio

from app.features.auth.registration_service import RegistrationService


class FakeRepo:
    def __init__(self):
        self.discord_docs = []
        self.linked_docs = []

    async def find_users_by_discord_id(self, discord_id: str, *, limit: int = 25):
        return list(self.discord_docs)

    async def find_users_by_linked_account_id(self, linked_account_id: str, *, limit: int = 25):
        return list(self.linked_docs)


def test_lookup_by_discord_id_returns_all_linked_account_hits():
    repo = FakeRepo()
    repo.discord_docs = [
        {
            "discord_id": "111111111111111111",
            "discord_username": "primary_test_user",
            "display_name": "Primary Test",
            "linked_platform": "steam",
            "linked_account_id": "76561190000000001",
            "linked_account_name": "Primary Steam",
        },
        {
            "discord_id": "111111111111111111",
            "discord_username": "primary_test_user",
            "display_name": "Primary Test",
            "linked_platform": "epic",
            "linked_account_id": "epic-test-001",
            "linked_account_name": "Primary Epic",
        },
    ]

    response = asyncio.run(RegistrationService(repo).lookup_by_discord_id("111111111111111111"))

    assert response is not None
    assert response.discord_id == "111111111111111111"
    assert response.discord_username == "primary_test_user"
    assert [hit.linked_account_id for hit in response.linked_accounts] == ["epic-test-001", "76561190000000001"]

def test_lookup_by_linked_account_id_returns_all_discord_hits_with_steam_compatibility():
    repo = FakeRepo()
    repo.linked_docs = [
        {
            "discord_id": "111111111111111111",
            "discord_username": "primary_test_user",
            "display_name": "Primary Test",
            "steam_id": "76561190000000001",
            "steam_name": "Primary Steam",
        },
        {
            "discord_id": "222222222222222222",
            "discord_username": "alt_test_user",
            "display_name": "Alt Test",
            "linked_platform": "steam",
            "linked_account_id": "76561190000000001",
            "linked_account_name": "Primary Steam",
        },
    ]

    response = asyncio.run(RegistrationService(repo).lookup_by_linked_account_id("76561190000000001"))

    assert response is not None
    assert response.linked_account_id == "76561190000000001"
    assert response.linked_platform is not None
    assert sorted(hit.discord_id for hit in response.discord_accounts) == [
        "111111111111111111",
        "222222222222222222",
    ]