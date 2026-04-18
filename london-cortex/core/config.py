"""Configuration: endpoints, thresholds, model choices, grid parameters."""

import os
from dataclasses import dataclass
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "graph.db"
MEMORY_DIR = DATA_DIR / "memory"
CACHE_DIR = DATA_DIR / "cache"
LOG_DIR = DATA_DIR / "logs"
STATIC_DIR = PROJECT_ROOT / "static"

# Ensure dirs exist
for d in (DATA_DIR, MEMORY_DIR, CACHE_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Grid ───────────────────────────────────────────────────────────────────────
GRID_CELL_SIZE_M = 500
LONDON_BBOX = {
    "min_lat": 51.28,
    "max_lat": 51.70,
    "min_lon": -0.51,
    "max_lon": 0.33,
}

# ── LLM Configuration ─────────────────────────────────────────────────────────
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")  # "gemini" or "glm"
LLM_FLASH_MODEL = os.getenv("LLM_FLASH_MODEL", "gemini-2.0-flash")
LLM_PRO_MODEL = os.getenv("LLM_PRO_MODEL", "gemini-2.5-pro")
LLM_RATE_LIMITS = {
    "flash": {"rpm": 60, "burst": 10},
    "pro": {"rpm": 30, "burst": 5},
}
BACKEND_PORT = 8000
FRONTEND_PORT = 3000

# ── Rate Limits ────────────────────────────────────────────────────────────────
@dataclass
class RateLimitConfig:
    gemini_flash_rpm: int = 60
    gemini_pro_rpm: int = 30
    tfl_rpm: int = 30
    default_rpm: int = 60

RATE_LIMITS = RateLimitConfig()

# ── Anomaly Thresholds ─────────────────────────────────────────────────────────
ANOMALY_Z_THRESHOLD = 2.0
ANOMALY_TTL_HOURS = 6

# ── Ingestor Intervals (seconds) ──────────────────────────────────────────────
INGESTOR_INTERVALS = {
    "tfl_jamcams": 300, "air_quality": 900, "weather": 1800, "met_office": 1800,
    "metoffice_obs": 1800, "news_gdelt": 900, "financial_stocks": 900,
    "financial_crypto": 1800, "polymarket": 1800, "carbon_intensity": 1800,
    "environment": 300, "thames_water": 900, "nature": 3600, "food_hygiene": 86400,
    "tfl_digital_health": 600, "sentinel2": 3600, "sentinel5p": 3600,
    "public_events": 3600, "road_disruptions": 300, "tfl_traffic": 300,
    "sensor_health": 900, "system_health": 900, "data_lineage": 900,
    "waze_traffic": 120, "apm": 60, "data_quality": 900, "tomtom_traffic": 300,
    "tfl_bus_congestion": 300, "provider_status": 300, "police_crimes": 3600,
    "planned_works": 600, "bus_avl": 120, "grid_generation": 300,
    "social_sentiment": 900, "cycle_hire": 300, "embedded_generation": 1800,
    "bus_crowding": 300, "retail_spending": 3600, "grid_demand": 1800,
    "ukpn_substation": 600, "ukpn_streetworks": 3600, "tfl_crowding": 600,
    "rideshare_demand": 600, "sensor_community": 900, "tfl_passenger_counts": 900,
    "commuter_displacement": 600, "tube_arrivals": 300, "micromobility": 600,
    "bluesky_transport": 600, "tfl_disruption_incidents": 300, "laqn_transport": 900,
    "river_flow": 300, "purpleair": 900, "google_aq": 1800, "opensensemap_mobile": 1800,
    "openaq": 900, "national_rail": 300, "tv_schedule": 3600, "nhs_syndromic": 3600,
    "windy_webcams": 540,
}

# ── Agent Intervals (seconds) ─────────────────────────────────────────────────
AGENT_INTERVALS = {
    "vision_interpreter": 300, "numeric_interpreter": 30, "text_interpreter": 60,
    "financial_interpreter": 60, "spatial_connector": 30, "narrative_connector": 120,
    "statistical_connector": 3600, "causal_chain_connector": 30, "brain": 300,
    "validator": 1800, "explorer_spawner": 120, "curiosity_engine": 600,
    "web_searcher": 60, "chronicler": 3600, "discovery_engine": 300,
}


def get_interval(name: str) -> tuple[float, float]:
    """Return (min_interval, max_interval) for a given interval name."""
    base = INGESTOR_INTERVALS.get(name) or AGENT_INTERVALS.get(name, 300)
    return (base * 0.5, base * 2.0)


# ── API Endpoints ──────────────────────────────────────────────────────────────
ENDPOINTS = {
    "tfl_jamcams": "https://api.tfl.gov.uk/Place/Type/JamCam",
    "laqn": "https://api.erg.ic.ac.uk/AirQuality/Hourly/MonitoringIndex/GroupName=London/Json",
    "open_meteo": "https://api.open-meteo.com/v1/forecast",
    "gdelt_geo": "https://api.gdeltproject.org/api/v2/geo/geo",
    "gdelt_doc": "https://api.gdeltproject.org/api/v2/doc/doc",
    "carbon_intensity": "https://api.carbonintensity.org.uk",
    "environment_agency": "https://environment.data.gov.uk/flood-monitoring",
    "polymarket_gamma": "https://gamma-api.polymarket.com",
    "coingecko": "https://api.coingecko.com/api/v3",
    "overpass": "https://overpass-api.de/api/interpreter",
    "google_aq": "https://airquality.googleapis.com/v1/currentConditions:lookup",
    "inaturalist": "https://api.inaturalist.org/v1",
    "food_hygiene": "https://api.ratings.food.gov.uk",
    "tfl_api_base": "https://api.tfl.gov.uk",
    "planetary_computer": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "predicthq": "https://api.predicthq.com/v1",
    "tfl_road_disruptions": "https://api.tfl.gov.uk/Road/all/Disruption",
    "tfl_road_status": "https://api.tfl.gov.uk/Road",
    "laqn_site_species": "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json",
    "ea_stations": "https://environment.data.gov.uk/flood-monitoring/id/stations",
    "waze_feed": "https://www.waze.com/partnerhub-api/waze-feeds/",  # requires WAZE_FEED_URL env var
    "tomtom_flow": "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json",
    "tomtom_incidents": "https://api.tomtom.com/traffic/services/5/incidentDetails",
    "tfl_bus_arrivals": "https://api.tfl.gov.uk/Line/{lineId}/Arrivals",
    "tfl_line_stops": "https://api.tfl.gov.uk/Line/{lineId}/StopPoints",
    "police_crimes": "https://data.police.uk/api/crimes-street/all-crime",
    "police_dates": "https://data.police.uk/api/crimes-street-dates",
    "tfl_line_status": "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line,tram/Status",
    "bods_siri_vm": "https://data.bus-data.dft.gov.uk/api/v1/datafeed/",
    "elexon_fuelinst": "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST",
    "reddit_search": "https://www.reddit.com/r/{subreddit}/search.json",
    "tfl_bikepoint": "https://api.tfl.gov.uk/BikePoint",
    "neso_embedded_forecast": "https://api.neso.energy/api/3/action/datastore_search",
    "ons_datasets": "https://api.beta.ons.gov.uk/v1/datasets",
    "elexon_indo": "https://data.elexon.co.uk/bmrs/api/v1/datasets/INDO",
    "neso_grid_forecast": "https://api.neso.energy/api/3/action/datastore_search",
    "neso_grid_status": "https://api.neso.energy/api/3/action/datastore_search",
    "ukpn_open_data": "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1",
    "tfl_crowding": "https://api.tfl.gov.uk/crowding/{naptan}",
    "uber_price_estimates": "https://api.uber.com/v1.2/estimates/price",
    "sensor_community": "https://data.sensor.community/airrohr/v1/filter/area=51.509,-0.118,25",
    "tfl_stop_crowding": "https://api.tfl.gov.uk/StopPoint/{naptan}",
    "tfl_journey_planner": "https://api.tfl.gov.uk/Journey/JourneyResults/{from}/to/{to}",
    "tfl_line_arrivals": "https://api.tfl.gov.uk/Line/{lineId}/Arrivals",
    "gbfs_lime": "https://data.lime.bike/api/partners/v2/gbfs/london/gbfs.json",
    "gbfs_dott": "https://gbfs.api.ridedott.com/public/v2/london/gbfs.json",
    "gbfs_tier": "https://platform.tier-services.io/v2/gbfs/london/gbfs.json",
    "bsky_search": "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
    "tfl_line_disruptions": "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line,tram/Disruption",
    "laqn_data_site_species": "https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies",
    "ea_flow_stations": "https://environment.data.gov.uk/flood-monitoring/id/stations?parameter=flow",
    "s5p_pal_stac": "https://data-portal.s5p-pal.com/api/s5p-l3",
    "purpleair": "https://api.purpleair.com/v1/sensors",
    "opensensemap_boxes": "https://api.opensensemap.org/boxes",
    "openaq": "https://api.openaq.org/v3/locations",
    "windy_webcams": "https://api.windy.com/webcams/api/v3/webcams",
    "darwin_ldbws": "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/ldb12.asmx",
    "tvmaze_schedule": "https://api.tvmaze.com/schedule",
    "tvmaze_web_schedule": "https://api.tvmaze.com/schedule/web",
    "ukhsa_dashboard": "https://api.ukhsa-dashboard.data.gov.uk",
}

# ── Message Board Channels ────────────────────────────────────────────────────
CHANNELS = ["#raw", "#observations", "#anomalies", "#hypotheses", "#requests", "#discoveries", "#meta", "#conversations"]

# ── Board Message TTL ──────────────────────────────────────────────────────────
DEFAULT_MESSAGE_TTL_HOURS = 6
