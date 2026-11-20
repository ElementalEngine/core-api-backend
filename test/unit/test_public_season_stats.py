from __future__ import annotations

from app.features.public.season_stats import mode_stats

NAMES = {"LEADER_A": "Ada", "LEADER_B": "Bo", "CIVILIZATION_X": "Xia"}


def _seat(leader, place, delta, civ="CIVILIZATION_X", **extra):
    return {"leader": leader, "civ": civ, "placement": place, "delta": delta, **extra}


def _ffa(map_type, seats, mode="ffa"):
    return {"game_mode": mode, "map_type": map_type, "players": seats}


def test_ffa_counts_picks_win_rates_and_best_leaders_per_map():
    games = [
        # Ada wins twice on Pangaea and finishes second once; Bo the reverse.
        _ffa("Pangaea", [_seat("LEADER_A", 0, 30.0), _seat("LEADER_B", 1, -30.0)]),
        _ffa("Pangaea", [_seat("LEADER_A", 0, 25.0), _seat("LEADER_B", 1, -25.0)]),
        _ffa("Pangaea", [_seat("LEADER_B", 0, 20.0), _seat("LEADER_A", 1, -20.0)]),
        # Stored capitalised by an older report; a sub's leaving seat is not his pick.
        _ffa(
            "Pangaea",
            [_seat("LEADER_A", 0, 5.0), _seat("LEADER_B", 1, 0.0, subbed_out=True)],
            mode="FFA",
        ),
        # A game with no map counts as a game, but not on any map.
        _ffa("Unknown", [_seat("LEADER_B", 0, 10.0), _seat("LEADER_C", 1, -10.0)]),
        _ffa("Lakes", [_seat("LEADER_A", 0, 10.0)], mode="duel"),
    ]
    setups = [
        {
            "mode": "ffa",
            "bans": [{"token": "LEADER_B", "kind": "leader", "by": "host"}],
        },
        {
            "mode": "duel",
            "bans": [{"token": "LEADER_A", "kind": "leader", "by": "host"}],
        },
    ]
    stats = mode_stats(
        mode="ffa",
        edition="civ6",
        matches=games,
        setups=setups,
        names=NAMES,
        min_games=4,
        top_n=1,
    )
    assert stats.games == 5
    assert [(m.map, m.games) for m in stats.maps] == [("Pangaea", 4)]
    assert [(b.name, b.bans) for b in stats.draft_bans] == [("Bo", 1)]
    leaders = {r.token: r for r in stats.leaders}
    assert (leaders["LEADER_A"].picks, leaders["LEADER_A"].wins) == (4, 3)
    assert leaders["LEADER_A"].win_rate == 0.75
    # Bo's seat in the game he left is not counted.
    assert (leaders["LEADER_B"].picks, leaders["LEADER_B"].win_rate) == (4, 0.5)
    # Below the minimum games, no win rate; with no name, the token.
    assert (leaders["LEADER_C"].name, leaders["LEADER_C"].win_rate) == (
        "LEADER_C",
        None,
    )
    assert stats.civs == []
    [pangaea] = stats.map_picks
    assert [(r.name, r.games, r.avg_placement) for r in pangaea.rows] == [
        ("Ada", 4, 1.25)
    ]


def test_civs_count_on_civ7_and_map_picks_only_on_ffa():
    games = [
        _ffa("Lakes", [_seat("LEADER_A", 0, 9.0), _seat("LEADER_B", 1, -9.0)], "duel")
    ]
    stats = mode_stats(
        mode="duel",
        edition="civ7",
        matches=games,
        setups=[],
        names=NAMES,
        min_games=1,
        top_n=5,
    )
    assert [(c.name, c.picks, c.wins) for c in stats.civs] == [("Xia", 2, 1)]
    assert stats.map_picks == []
