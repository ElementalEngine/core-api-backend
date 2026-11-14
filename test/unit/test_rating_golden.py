"""The rating maths, frozen: six matches through approve's rating step must give
these exact results, measured on production's v1 and v2 code alike."""

from __future__ import annotations

import pytest

import app.features.matches.service as service_module
import app.features.ratings.skill as skill_module
from app.core.config import Settings
from app.features.matches.models import MatchModel, PlayerModel, StatModel
from app.features.matches.service import MatchService

# Production's rating settings; they are also the defaults.
FROZEN = {
    "ts_mu": 1200.0,
    "ts_sigma": 150.0,
    "ts_beta": 400.0,
    "ts_tau": 10.0,
    "ts_draw_prob": 0.0,
    "min_points_for_subs": 5,
}

# seat: (team, placement, mu, sigma, is_sub, subbed_out)
CASES = {
    "ffa6": [
        (0, 2, 1400, 80, 0, 0),
        (1, 0, 1300, 100, 0, 0),
        (2, 5, 1250, 150, 0, 0),
        (3, 1, 1200, 90, 0, 0),
        (4, 4, 1100, 120, 0, 0),
        (5, 3, 1050, 60, 0, 0),
    ],
    "ffa4_tie": [
        (0, 0, 1260, 70, 0, 0),
        (1, 1, 1240, 110, 0, 0),
        (2, 1, 1310, 95, 0, 0),
        (3, 3, 1180, 130, 0, 0),
    ],
    "teamer2v2": [
        (0, 1, 1350, 70, 0, 0),
        (0, 1, 1300, 90, 0, 0),
        (1, 0, 1200, 110, 0, 0),
        (1, 0, 1150, 140, 0, 0),
    ],
    "duel_upset": [(0, 1, 1500, 60, 0, 0), (1, 0, 1150, 140, 0, 0)],
    "ffa5_sub": [
        (0, 0, 1280, 85, 0, 0),
        (1, 1, 1330, 75, 0, 1),
        (1, 1, 1190, 125, 1, 0),
        (2, 2, 1220, 100, 0, 0),
        (3, 3, 1270, 65, 0, 0),
    ],
    "teamer3v3_sub": [
        (0, 0, 1300, 80, 0, 0),
        (0, 0, 1250, 95, 0, 0),
        (0, 0, 1210, 120, 0, 0),
        (1, 1, 1320, 70, 0, 0),
        (1, 1, 1280, 88, 0, 1),
        (1, 1, 1150, 135, 1, 0),
    ],
}

# case: [(delta, mu after, sigma after)] in seat order
GOLDEN = {
    "ffa6": [
        (-4, 1396.0, 79.45654153152435),
        (28, 1328.0, 98.79970169540451),
        (-73, 1177.0, 144.62147450610448),
        (14, 1214.0, 88.97838205643093),
        (-16, 1084.0, 116.79659443586338),
        (1, 1051.0, 60.32371600155153),
    ],
    "ffa4_tie": [
        (10, 1270.0, 70.18248685322105),
        (1, 1241.0, 107.72113474287322),
        (-3, 1307.0, 93.7454277078103),
        (-29, 1151.0, 127.34881353402082),
    ],
    "teamer2v2": [
        (-6, 1344.0, 70.52771434005884),
        (-10, 1290.0, 90.16926679746598),
        (15, 1215.0, 109.75495426749592),
        (25, 1175.0, 138.9202891440821),
    ],
    "duel_upset": [(-8, 1492.0, 60.58238889967823), (41, 1191.0, 137.31696712294703)],
    "ffa5_sub": [
        (19, 1299.0, 84.62687811681765),
        (0, 1330.0, 74.8349161416928),
        (15, 1205.0, 121.7058903989455),
        (-4, 1216.0, 98.57362977101441),
        (-11, 1259.0, 65.3260141385167),
    ],
    "teamer3v3_sub": [
        (1, 1301.0, 80.53141352060531),
        (2, 1252.0, 95.3731951436609),
        (3, 1213.0, 120.1119916762617),
        (-1, 1319.0, 70.64918147001197),
        (-2, 1278.0, 88.44549157016897),
        (5, 1155.0, 134.99793362002944),
    ],
}


@pytest.fixture
def frozen_settings(monkeypatch):
    for module in (service_module, skill_module):
        for name, value in FROZEN.items():
            monkeypatch.setattr(module.settings, name, value)


def test_defaults_are_production_settings():
    assert {name: Settings.model_fields[name].default for name in FROZEN} == FROZEN


@pytest.mark.parametrize("case", list(CASES))
def test_approve_rates_as_frozen(case, frozen_settings):
    seats = CASES[case]
    players = [
        PlayerModel(
            civ="X",
            team=t,
            placement=pl,
            discord_id=str(1001 + i),
            is_sub=bool(s),
            subbed_out=bool(o),
        )
        for i, (t, pl, _m, _s, s, o) in enumerate(seats)
    ]
    match = MatchModel(
        game="civ6",
        turn=100,
        map_type="Pangaea",
        game_mode="FFA",
        is_cloud=False,
        players=players,
        parser_version="2.0",
        discord_messages_id_list=[],
        save_file_hash="x",
        reporter_discord_id="1001",
        contest_report_list=[],
    )
    ranking = [
        StatModel(
            index=i,
            discord_id=str(1001 + i),
            mu=float(m),
            sigma=float(sg),
            games=10,
            wins=5,
            first=2,
        )
        for i, (_t, _p, m, sg, _s, _o) in enumerate(seats)
    ]
    match, post = MatchService.update_player_stats(None, match, ranking, "delta")
    got = [(p.delta, post[i].mu, post[i].sigma) for i, p in enumerate(match.players)]
    assert [(d, m) for d, m, _ in got] == [(d, m) for d, m, _ in GOLDEN[case]]
    assert [s for _, _, s in got] == pytest.approx(
        [s for _, _, s in GOLDEN[case]], rel=1e-12
    )
