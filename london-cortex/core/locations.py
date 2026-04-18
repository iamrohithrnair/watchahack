"""Location resolver — landmark/station/area names to grid cells."""

from __future__ import annotations

from typing import Any

from .graph import CortexGraph

# ~200 London landmarks, stations, areas, parks, hospitals, embassies → (lat, lon)
LONDON_LANDMARKS: dict[str, tuple[float, float]] = {
    # Major landmarks
    "big ben": (51.5007, -0.1246),
    "parliament": (51.4995, -0.1248),
    "houses of parliament": (51.4995, -0.1248),
    "westminster abbey": (51.4993, -0.1273),
    "buckingham palace": (51.5014, -0.1419),
    "tower of london": (51.5081, -0.0759),
    "tower bridge": (51.5055, -0.0754),
    "london bridge": (51.5079, -0.0877),
    "london eye": (51.5033, -0.1195),
    "the shard": (51.5045, -0.0865),
    "st pauls cathedral": (51.5138, -0.0984),
    "st pauls": (51.5138, -0.0984),
    "trafalgar square": (51.5080, -0.1281),
    "piccadilly circus": (51.5100, -0.1340),
    "leicester square": (51.5112, -0.1281),
    "covent garden": (51.5117, -0.1240),
    "hyde park": (51.5073, -0.1657),
    "regent street": (51.5117, -0.1390),
    "oxford street": (51.5152, -0.1418),
    "oxford circus": (51.5152, -0.1418),
    "bond street": (51.5142, -0.1494),
    "marble arch": (51.5131, -0.1589),
    "speakers corner": (51.5121, -0.1590),
    "kensington palace": (51.5050, -0.1877),
    "natural history museum": (51.4967, -0.1764),
    "science museum": (51.4978, -0.1745),
    "v&a museum": (51.4966, -0.1722),
    "victoria and albert museum": (51.4966, -0.1722),
    "british museum": (51.5194, -0.1270),
    "tate modern": (51.5076, -0.0994),
    "tate britain": (51.4910, -0.1278),
    "national gallery": (51.5089, -0.1283),
    "royal albert hall": (51.5009, -0.1774),
    "albert memorial": (51.5024, -0.1779),
    "millennium bridge": (51.5095, -0.0985),
    "globe theatre": (51.5081, -0.0972),
    "borough market": (51.5055, -0.0910),
    "southbank centre": (51.5068, -0.1162),
    "royal festival hall": (51.5068, -0.1162),
    "barbican": (51.5200, -0.0937),
    "bank of england": (51.5142, -0.0886),
    "mansion house": (51.5126, -0.0937),
    "guildhall": (51.5155, -0.0921),
    "monument": (51.5101, -0.0860),
    "leadenhall market": (51.5130, -0.0833),
    "lloyds building": (51.5133, -0.0825),
    "gherkin": (51.5145, -0.0803),
    "walkie talkie": (51.5112, -0.0834),
    "cheesegrater": (51.5147, -0.0812),
    "canary wharf": (51.5054, -0.0235),
    "greenwich": (51.4769, -0.0005),
    "royal observatory": (51.4769, 0.0005),
    "o2 arena": (51.5030, 0.0032),
    "millennium dome": (51.5030, 0.0032),
    "excel centre": (51.5085, 0.0292),
    "excel london": (51.5085, 0.0292),
    "olympic park": (51.5430, -0.0134),
    "queen elizabeth olympic park": (51.5430, -0.0134),
    "wembley stadium": (51.5560, -0.2796),
    "lords cricket ground": (51.5294, -0.1728),
    "the oval": (51.4837, -0.1150),
    "wimbledon": (51.4341, -0.2143),
    "hampton court palace": (51.4036, -0.3378),
    "kew gardens": (51.4787, -0.2956),
    "richmond park": (51.4432, -0.2749),
    "regents park": (51.5313, -0.1570),
    "primrose hill": (51.5395, -0.1600),
    "camden market": (51.5413, -0.1471),
    "camden town": (51.5392, -0.1426),
    "kings cross": (51.5305, -0.1233),
    "st pancras": (51.5305, -0.1270),
    "euston": (51.5282, -0.1337),
    "paddington": (51.5154, -0.1755),
    "victoria station": (51.4952, -0.1441),
    "waterloo station": (51.5031, -0.1132),
    "charing cross": (51.5073, -0.1244),
    "liverpool street": (51.5178, -0.0823),
    "fenchurch street": (51.5119, -0.0787),
    "moorgate": (51.5186, -0.0886),
    "cannon street": (51.5113, -0.0904),
    "blackfriars": (51.5120, -0.1038),
    "embankment": (51.5074, -0.1223),
    "temple": (51.5114, -0.1139),
    "holborn": (51.5174, -0.1199),
    "chancery lane": (51.5185, -0.1114),
    "tottenham court road": (51.5163, -0.1310),
    "warren street": (51.5245, -0.1385),
    "goodge street": (51.5206, -0.1345),
    "angel": (51.5322, -0.1058),
    "old street": (51.5263, -0.0879),
    "shoreditch": (51.5246, -0.0767),
    "brick lane": (51.5219, -0.0718),
    "whitechapel": (51.5194, -0.0600),
    "bethnal green": (51.5270, -0.0555),
    "mile end": (51.5252, -0.0332),
    "bow": (51.5269, -0.0206),
    "stratford": (51.5416, -0.0036),
    "west ham": (51.5282, 0.0053),
    "east ham": (51.5394, 0.0518),
    "barking": (51.5396, 0.0808),
    "woolwich": (51.4907, 0.0693),
    "lewisham": (51.4613, -0.0139),
    "brixton": (51.4613, -0.1145),
    "clapham": (51.4620, -0.1380),
    "battersea": (51.4754, -0.1476),
    "battersea power station": (51.4818, -0.1467),
    "chelsea": (51.4876, -0.1685),
    "fulham": (51.4754, -0.2000),
    "hammersmith": (51.4927, -0.2230),
    "shepherds bush": (51.5046, -0.2265),
    "notting hill": (51.5100, -0.1982),
    "portobello road": (51.5152, -0.2050),
    "ladbroke grove": (51.5172, -0.2105),
    "bayswater": (51.5120, -0.1879),
    "maida vale": (51.5264, -0.1860),
    "kilburn": (51.5472, -0.1919),
    "finchley road": (51.5472, -0.1808),
    "swiss cottage": (51.5432, -0.1752),
    "hampstead": (51.5563, -0.1778),
    "hampstead heath": (51.5614, -0.1640),
    "highgate": (51.5714, -0.1458),
    "archway": (51.5655, -0.1345),
    "holloway": (51.5558, -0.1204),
    "islington": (51.5362, -0.1033),
    "hackney": (51.5450, -0.0553),
    "dalston": (51.5462, -0.0755),
    "stoke newington": (51.5628, -0.0742),
    "tottenham": (51.5882, -0.0717),
    "wood green": (51.5975, -0.1096),
    "finsbury park": (51.5642, -0.1065),
    "crouch end": (51.5771, -0.1240),
    "muswell hill": (51.5903, -0.1443),
    "ealing": (51.5136, -0.3059),
    "acton": (51.5088, -0.2672),
    "chiswick": (51.4923, -0.2625),
    "richmond": (51.4613, -0.3037),
    "putney": (51.4594, -0.2166),
    "wandsworth": (51.4566, -0.1909),
    "tooting": (51.4276, -0.1681),
    "balham": (51.4433, -0.1524),
    "streatham": (51.4281, -0.1312),
    "dulwich": (51.4484, -0.0849),
    "peckham": (51.4738, -0.0690),
    "camberwell": (51.4740, -0.0929),
    "elephant and castle": (51.4945, -0.1004),
    "bermondsey": (51.4979, -0.0638),
    "rotherhithe": (51.4998, -0.0519),
    "deptford": (51.4743, -0.0261),
    "new cross": (51.4762, -0.0391),
    "catford": (51.4449, -0.0206),
    "forest hill": (51.4393, -0.0531),
    "crystal palace": (51.4186, -0.0760),
    "croydon": (51.3762, -0.0986),
    "bromley": (51.4029, 0.0148),
    "eltham": (51.4515, 0.0521),
    "blackheath": (51.4661, 0.0097),
    "docklands": (51.5054, -0.0235),
    "isle of dogs": (51.4975, -0.0178),
    "limehouse": (51.5124, -0.0388),
    "wapping": (51.5065, -0.0574),
    "city of london": (51.5155, -0.0922),
    "the city": (51.5155, -0.0922),
    "square mile": (51.5155, -0.0922),
    "soho": (51.5137, -0.1337),
    "mayfair": (51.5094, -0.1480),
    "westminster": (51.4975, -0.1357),
    "pimlico": (51.4893, -0.1337),
    "vauxhall": (51.4861, -0.1225),
    "nine elms": (51.4820, -0.1380),
    "lambeth": (51.4907, -0.1167),
    "southwark": (51.5035, -0.1004),
    "kennington": (51.4880, -0.1061),
    "stockwell": (51.4720, -0.1231),
    "oval": (51.4816, -0.1131),

    # Major hospitals
    "st thomas hospital": (51.4987, -0.1184),
    "guys hospital": (51.5037, -0.0872),
    "royal london hospital": (51.5180, -0.0590),
    "ucl hospital": (51.5246, -0.1340),
    "great ormond street": (51.5228, -0.1196),
    "chelsea and westminster hospital": (51.4842, -0.1818),
    "kings college hospital": (51.4686, -0.0939),
    "imperial college hospital": (51.5165, -0.1749),

    # Embassies
    "iranian embassy": (51.4985, -0.1660),
    "us embassy": (51.4824, -0.1287),
    "american embassy": (51.4824, -0.1287),
    "russian embassy": (51.5030, -0.1850),
    "french embassy": (51.5050, -0.1530),
    "chinese embassy": (51.5142, -0.1921),
    "israeli embassy": (51.5073, -0.1630),
    "saudi embassy": (51.5086, -0.1498),

    # Parks
    "green park": (51.5067, -0.1428),
    "st james park": (51.5025, -0.1340),
    "victoria park": (51.5363, -0.0394),
    "battersea park": (51.4791, -0.1564),
    "clapham common": (51.4588, -0.1380),
    "tooting common": (51.4326, -0.1559),
    "greenwich park": (51.4769, -0.0005),
    "finsbury park": (51.5642, -0.1065),
    "alexandra palace": (51.5940, -0.1300),

    # Boroughs
    "city of westminster": (51.4975, -0.1357),
    "tower hamlets": (51.5150, -0.0400),
    "hackney borough": (51.5450, -0.0553),
    "camden borough": (51.5290, -0.1255),
    "islington borough": (51.5362, -0.1033),
    "haringey": (51.5882, -0.1096),
    "barnet": (51.6500, -0.2000),
    "enfield": (51.6522, -0.0808),
    "waltham forest": (51.5886, -0.0118),
    "redbridge": (51.5590, 0.0741),
    "havering": (51.5779, 0.2121),
    "bexley": (51.4613, 0.1497),
    "greenwich borough": (51.4769, -0.0005),
    "lewisham borough": (51.4613, -0.0139),
    "southwark borough": (51.5035, -0.1004),
    "lambeth borough": (51.4907, -0.1167),
    "wandsworth borough": (51.4566, -0.1909),
    "merton": (51.4098, -0.2108),
    "kingston": (51.4123, -0.3007),
    "richmond borough": (51.4613, -0.3037),
    "hounslow": (51.4746, -0.3680),
    "hillingdon": (51.5441, -0.4760),
    "ealing borough": (51.5136, -0.3059),
    "brent": (51.5588, -0.2817),
    "harrow": (51.5898, -0.3346),
    "barking and dagenham": (51.5396, 0.0808),
    "newham": (51.5282, 0.0296),
    "sutton": (51.3618, -0.1945),
    "croydon borough": (51.3762, -0.0986),
    "bromley borough": (51.4029, 0.0148),

    # Transport hubs
    "heathrow": (51.4700, -0.4543),
    "heathrow airport": (51.4700, -0.4543),
    "gatwick": (51.1537, -0.1821),
    "city airport": (51.5053, 0.0553),
    "london city airport": (51.5053, 0.0553),
    "stansted": (51.8860, 0.2389),
    "luton airport": (51.8747, -0.3683),
}


def resolve_location(name: str, graph: CortexGraph) -> dict[str, Any] | None:
    """Resolve a location name to a grid cell.

    Returns {"cell_id", "lat", "lon", "name"} or None.
    """
    if not name:
        return None

    query = name.strip().lower()

    # 1. Exact match
    if query in LONDON_LANDMARKS:
        lat, lon = LONDON_LANDMARKS[query]
        cell_id = graph.latlon_to_cell(lat, lon)
        if cell_id:
            return {"cell_id": cell_id, "lat": lat, "lon": lon, "name": name}

    # 2. Fuzzy substring match — find best match
    best_match = None
    best_len = 0
    for landmark, (lat, lon) in LONDON_LANDMARKS.items():
        if query in landmark or landmark in query:
            # Prefer longer matches (more specific)
            if len(landmark) > best_len:
                best_len = len(landmark)
                best_match = (landmark, lat, lon)

    if best_match:
        landmark, lat, lon = best_match
        cell_id = graph.latlon_to_cell(lat, lon)
        if cell_id:
            return {"cell_id": cell_id, "lat": lat, "lon": lon, "name": landmark}

    return None
