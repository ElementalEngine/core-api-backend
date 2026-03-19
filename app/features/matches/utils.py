from app.features.matches.parsers.civ6leaders import civ6_leaders_dict
from app.features.matches.parsers.civ7leaders import civ7_leaders_dict


def get_cpl_name(civ_name):
    if civ_name in civ6_leaders_dict.keys():
        return civ6_leaders_dict[civ_name]
    elif civ_name in civ7_leaders_dict.keys():
        return civ7_leaders_dict[civ_name]
    elif civ_name.startswith('LOC_LEADER_'):
        try:
            leader = civ_name[11:]
            if leader in civ6_leaders_dict.keys():
                return civ6_leaders_dict[leader]
        except Exception:
            pass
    elif civ_name.startswith('LEADER_'):
        try:
            leader = civ_name[7:]
            if leader in civ6_leaders_dict.keys():
                return civ6_leaders_dict[leader]
        except Exception:
            pass
    elif civ_name.startswith('LOC_CIVILIZATION_'):
        try:
            civ = civ_name[17:]
            if civ in civ7_leaders_dict.keys():
                return civ7_leaders_dict[civ]
        except Exception:
            pass
    elif civ_name.startswith('CIVILIZATION_'):
        try:
            civ = civ_name[13:]
            if civ in civ7_leaders_dict.keys():
                return civ7_leaders_dict[civ]
        except Exception:
            pass
    return civ_name


__all__ = ["get_cpl_name"]
