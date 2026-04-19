"""Declarative ingestor registry — replaces repetitive registration code."""

from __future__ import annotations

import os
from typing import Any

# (func_name, module_path, interval_key, priority, source_names, required_env_vars)
INGESTOR_REGISTRY: list[tuple[str, str, str, int, list[str], list[str]]] = [
    # Traffic & Mobility
    ("ingest_tfl", "cortex.ingestors.tfl", "tfl_jamcams", 2, ["tfl_jamcam", "tfl"], []),
    ("ingest_tfl_traffic", "cortex.ingestors.tfl_traffic", "tfl_traffic", 2, ["tfl_traffic"], []),
    ("ingest_tfl_crowding", "cortex.ingestors.tfl_crowding", "tfl_crowding", 2, ["tfl_crowding"], []),
    ("ingest_tfl_bus_congestion", "cortex.ingestors.tfl_bus_congestion", "tfl_bus_congestion", 3, ["tfl_bus_congestion"], []),
    ("ingest_tfl_disruption_incidents", "cortex.ingestors.tfl_disruption_incidents", "tfl_disruption_incidents", 2, ["tfl_disruption_incidents"], []),
    ("ingest_tfl_passenger_counts", "cortex.ingestors.tfl_passenger_counts", "tfl_passenger_counts", 3, ["tfl_passenger_counts"], []),
    ("ingest_road_disruptions", "cortex.ingestors.road_disruptions", "road_disruptions", 2, ["tfl_road_disruptions"], []),
    ("ingest_planned_works", "cortex.ingestors.planned_works", "planned_works", 3, ["tfl_planned_works"], []),
    ("ingest_bus_avl", "cortex.ingestors.bus_avl", "bus_avl", 2, ["bods_bus_avl"], ["BODS_API_KEY"]),
    ("ingest_bus_crowding", "cortex.ingestors.bus_crowding", "bus_crowding", 2, ["tfl_bus_crowding"], []),
    ("ingest_tube_arrivals", "cortex.ingestors.tube_arrivals", "tube_arrivals", 2, ["tube_arrivals"], []),
    ("ingest_cycle_hire", "cortex.ingestors.cycle_hire", "cycle_hire", 3, ["tfl_cycle_hire"], []),
    ("ingest_micromobility", "cortex.ingestors.micromobility", "micromobility", 4, ["micromobility"], []),
    ("ingest_national_rail", "cortex.ingestors.national_rail", "national_rail", 2, ["national_rail"], []),
    ("ingest_commuter_displacement", "cortex.ingestors.commuter_displacement", "commuter_displacement", 3, ["commuter_displacement"], []),

    # Traffic Intelligence
    ("ingest_rideshare_demand", "cortex.ingestors.rideshare_demand", "rideshare_demand", 4, ["rideshare_demand"], ["UBER_SERVER_TOKEN"]),
    ("ingest_waze_traffic", "cortex.ingestors.waze_traffic", "waze_traffic", 2, ["waze_traffic"], ["WAZE_FEED_URL"]),
    ("ingest_tomtom_traffic", "cortex.ingestors.tomtom_traffic", "tomtom_traffic", 2, ["tomtom_traffic"], ["TOMTOM_API_KEY"]),

    # Air Quality & Environment
    ("ingest_air_quality", "cortex.ingestors.air_quality", "air_quality", 3, ["laqn"], []),
    ("ingest_laqn_transport", "cortex.ingestors.laqn_transport", "laqn_transport", 3, ["laqn_transport"], []),
    ("ingest_purpleair", "cortex.ingestors.purpleair", "purpleair", 5, ["purpleair"], ["PURPLEAIR_API_KEY"]),
    ("ingest_google_air_quality", "cortex.ingestors.google_air_quality", "google_aq", 5, ["google_aq"], ["GOOGLE_AQ_API_KEY"]),
    ("ingest_opensensemap_mobile", "cortex.ingestors.opensensemap_mobile", "opensensemap_mobile", 5, ["opensensemap_mobile"], []),
    ("ingest_openaq", "cortex.ingestors.openaq", "openaq", 5, ["openaq"], ["OPENAQ_API_KEY"]),
    ("ingest_sensor_community", "cortex.ingestors.sensor_community", "sensor_community", 5, ["sensor_community"], []),
    ("ingest_environment", "cortex.ingestors.environment", "environment", 5, ["environment_agency"], []),
    ("ingest_river_flow", "cortex.ingestors.river_flow", "river_flow", 5, ["ea_river_flow"], []),
    ("ingest_thames_water", "cortex.ingestors.thames_water", "thames_water", 5, ["thames_water_edm"], []),

    # Weather
    ("ingest_weather", "cortex.ingestors.weather", "weather", 4, ["open_meteo"], []),
    ("ingest_met_office", "cortex.ingestors.met_office", "met_office", 4, ["met_office"], []),

    # Energy & Grid
    ("ingest_energy", "cortex.ingestors.energy", "carbon_intensity", 6, ["carbon_intensity"], []),
    ("ingest_grid_generation", "cortex.ingestors.grid_generation", "grid_generation", 4, ["grid_generation"], []),
    ("ingest_grid_demand", "cortex.ingestors.grid_demand", "grid_demand", 5, ["grid_demand"], []),
    ("ingest_grid_status", "cortex.ingestors.grid_status", "grid_status", 5, ["neso_grid_status"], []),
    ("ingest_grid_forecast", "cortex.ingestors.grid_forecast", "grid_forecast", 4, ["neso_grid_forecast"], []),
    ("ingest_embedded_generation", "cortex.ingestors.embedded_generation", "embedded_generation", 5, ["neso_embedded_gen"], []),
    ("ingest_ukpn_substation", "cortex.ingestors.ukpn_substation", "ukpn_substation", 4, ["ukpn_substation"], []),

    # Financial
    ("ingest_financial", "cortex.ingestors.financial", "financial_stocks", 5, ["financial_stocks", "financial_crypto", "polymarket"], []),

    # News & Social
    ("ingest_news", "cortex.ingestors.news", "news_gdelt", 3, ["gdelt", "news"], []),
    ("ingest_social_sentiment", "cortex.ingestors.social_sentiment", "social_sentiment", 5, ["social_sentiment"], []),

    # Satellite & Imagery
    ("ingest_satellite", "cortex.ingestors.satellite", "sentinel2", 9, ["sentinel2"], []),
    ("ingest_sentinel5p", "cortex.ingestors.sentinel5p", "sentinel5p", 8, ["sentinel5p"], []),
    ("ingest_tfl_digital_health", "cortex.ingestors.tfl_digital_health", "tfl_digital_health", 3, ["tfl_digital_health"], []),

    # Events & Culture
    ("ingest_events", "cortex.ingestors.events", "public_events", 5, ["public_events"], ["PREDICTHQ_API_KEY"]),
    ("ingest_vam_museum", "cortex.ingestors.vam_museum", "vam_museum", 8, ["vam_museum"], []),
    ("ingest_uk_parliament", "cortex.ingestors.uk_parliament", "uk_parliament", 7, ["uk_parliament"], []),
    ("ingest_companies_house", "cortex.ingestors.companies_house", "companies_house", 8, ["uk_petitions"], []),
    ("ingest_historic_england", "cortex.ingestors.historic_england", "historic_england", 9, ["ons_demographics"], []),
    ("ingest_open_plaques", "cortex.ingestors.open_plaques", "open_plaques", 9, ["open_plaques"], []),
    ("ingest_tv_schedule", "cortex.ingestors.tv_schedule", "tv_schedule", 7, ["tv_schedule"], []),

    # Public Services
    ("ingest_police_crimes", "cortex.ingestors.police_crimes", "police_crimes", 6, ["police_crimes"], []),
    ("ingest_nhs_syndromic", "cortex.ingestors.nhs_syndromic", "nhs_syndromic", 5, ["nhs_syndromic"], []),
    ("ingest_land_registry", "cortex.ingestors.land_registry", "land_registry", 7, ["land_registry"], []),

    # Nature & Wildlife
    ("ingest_nature", "cortex.ingestors.nature", "nature", 7, ["nature_inaturalist"], []),
    ("ingest_misc", "cortex.ingestors.misc", "food_hygiene", 8, ["food_hygiene"], []),

    # System Health
    ("ingest_system_health", "cortex.ingestors.system_health", "system_health", 4, ["system_health"], []),
    ("ingest_sensor_health", "cortex.ingestors.sensor_health", "sensor_health", 4, ["sensor_health"], []),
    ("ingest_provider_status", "cortex.ingestors.provider_status", "provider_status", 3, ["provider_status"], []),
    ("ingest_data_lineage", "cortex.ingestors.data_lineage", "data_lineage", 4, ["data_lineage"], []),
    ("ingest_data_quality", "cortex.ingestors.data_quality", "data_quality", 4, ["data_quality"], []),
    ("ingest_apm", "cortex.ingestors.apm", "apm", 3, ["apm"], []),

    # Other
    ("ingest_retail_spending", "cortex.ingestors.retail_spending", "retail_spending", 6, ["retail_spending"], []),
    ("ingest_windy_webcams", "cortex.ingestors.windy_webcams", "windy_webcams", 3, ["windy_webcam"], []),

    # ── New ingestors from london-nervous-system ───────────────────────────────

    # News & Media
    ("ingest_bbc_london_news", "cortex.ingestors.bbc_london_news", "bbc_london_news", 3, ["bbc_london_news"], []),
    ("ingest_guardian_news", "cortex.ingestors.guardian_news", "guardian_news", 3, ["guardian_news"], []),
    ("ingest_uk_headlines", "cortex.ingestors.uk_headlines", "uk_headlines", 4, ["uk_headlines"], []),
    ("ingest_met_police_news", "cortex.ingestors.met_police_news", "met_police_news", 4, ["met_police_news"], []),
    ("ingest_gdelt_events", "cortex.ingestors.gdelt_events", "gdelt_events", 3, ["gdelt_events"], []),
    ("ingest_gdelt_discourse", "cortex.ingestors.gdelt_discourse", "gdelt_discourse", 5, ["gdelt_discourse"], []),

    # Government & Politics
    ("ingest_hansard", "cortex.ingestors.hansard", "hansard", 7, ["hansard"], []),
    ("ingest_parliament_petitions", "cortex.ingestors.parliament_petitions", "parliament_petitions", 6, ["parliament_petitions"], []),
    ("ingest_parliament_calendar", "cortex.ingestors.parliament_calendar", "parliament_calendar", 7, ["parliament_calendar"], []),
    ("ingest_gov_press", "cortex.ingestors.gov_press", "gov_press", 5, ["gov_press"], []),
    ("ingest_gov_notices", "cortex.ingestors.gov_notices", "gov_notices", 6, ["gov_notices"], []),
    ("ingest_london_assembly", "cortex.ingestors.london_assembly", "london_assembly", 7, ["london_assembly"], []),
    ("ingest_gla_events", "cortex.ingestors.gla_events", "gla_events", 6, ["gla_events"], []),
    ("ingest_gla_press", "cortex.ingestors.gla_press", "gla_press", 6, ["gla_press"], []),
    ("ingest_court_judgments", "cortex.ingestors.court_judgments", "court_judgments", 7, ["court_judgments"], []),
    ("ingest_acled_protests", "cortex.ingestors.acled_protests", "acled_protests", 5, ["acled_protests"], ["ACLED_API_KEY", "ACLED_EMAIL"]),
    ("ingest_industrial_action", "cortex.ingestors.industrial_action", "industrial_action", 5, ["industrial_action"], []),
    ("ingest_police_stop_search", "cortex.ingestors.police_stop_search", "police_stop_search", 6, ["police_stop_search"], []),

    # Infrastructure & Energy
    ("ingest_electricity_prices", "cortex.ingestors.electricity_prices", "electricity_prices", 5, ["electricity_prices"], []),
    ("ingest_street_manager", "cortex.ingestors.street_manager", "street_manager", 4, ["street_manager"], ["STREET_MANAGER_EMAIL", "STREET_MANAGER_PASSWORD"]),
    ("ingest_network_outages", "cortex.ingestors.network_outages", "network_outages", 4, ["network_outages"], ["CLOUDFLARE_RADAR_API_TOKEN"]),
    ("ingest_cloudflare_radar", "cortex.ingestors.cloudflare_radar", "cloudflare_radar", 4, ["cloudflare_radar"], ["CLOUDFLARE_RADAR_API_TOKEN"]),
    ("ingest_infrastructure_status", "cortex.ingestors.infrastructure_status", "infrastructure_status", 4, ["infrastructure_status"], []),
    ("ingest_smart_energy", "cortex.ingestors.smart_energy", "smart_energy", 6, ["smart_energy"], []),
    ("ingest_commodities", "cortex.ingestors.commodities", "commodities", 5, ["commodities"], []),

    # Places & POIs
    ("ingest_osm_poi", "cortex.ingestors.osm_poi", "osm_poi", 8, ["osm_poi"], []),
    ("ingest_corporate_hq", "cortex.ingestors.corporate_hq", "corporate_hq", 8, ["corporate_hq"], []),
    ("ingest_planning", "cortex.ingestors.planning", "planning", 7, ["planning"], []),
    ("ingest_pla_tides", "cortex.ingestors.pla_tides", "pla_tides", 6, ["pla_tides"], []),
    ("ingest_flight_tracker", "cortex.ingestors.flight_tracker", "flight_tracker", 5, ["flight_tracker"], []),

    # Events & Entertainment
    ("ingest_sporting_events", "cortex.ingestors.sporting_events", "sporting_events", 5, ["sporting_events"], []),
    ("ingest_skiddle_events", "cortex.ingestors.skiddle_events", "skiddle_events", 6, ["skiddle_events"], []),
    ("ingest_ticketmaster_events", "cortex.ingestors.ticketmaster_events", "ticketmaster_events", 6, ["ticketmaster_events"], []),

    # Transport supplementary
    ("ingest_cycle_rebalancing", "cortex.ingestors.cycle_rebalancing", "cycle_rebalancing", 5, ["cycle_rebalancing"], []),
    ("ingest_bt_footfall", "cortex.ingestors.bt_footfall", "bt_footfall", 5, ["bt_footfall"], ["BT_FOOTFALL_API_KEY"]),

    # Environment supplementary
    ("ingest_met_warnings", "cortex.ingestors.met_warnings", "met_warnings", 4, ["met_warnings"], []),
    ("ingest_metoffice_obs", "cortex.ingestors.metoffice_obs", "metoffice_obs", 4, ["metoffice_obs"], []),
    ("ingest_breathe_london", "cortex.ingestors.breathe_london", "breathe_london", 5, ["breathe_london"], []),
    ("ingest_crowd_density", "cortex.ingestors.crowd_density", "crowd_density", 4, ["crowd_density"], ["BESTTIME_API_KEY"]),

    # Social & Culture
    ("ingest_flickr_geo_media", "cortex.ingestors.flickr_geo_media", "flickr_geo_media", 7, ["flickr_geo_media"], ["FLICKR_API_KEY"]),
    ("ingest_social_topics", "cortex.ingestors.social_topics", "social_topics", 5, ["social_topics"], ["TEXTRAZOR_API_KEY"]),
    ("ingest_worker_sentiment", "cortex.ingestors.worker_sentiment", "worker_sentiment", 5, ["worker_sentiment"], []),
]

# Agent registry: (task_name, module_path, func_name, interval_key, priority)
AGENT_REGISTRY: list[tuple[str, str, str, str, int]] = [
    ("vision_interpreter", "cortex.agents.interpreters", "run_vision_interpreter", "vision_interpreter", 2),
    ("numeric_interpreter", "cortex.agents.interpreters", "run_numeric_interpreter", "numeric_interpreter", 2),
    ("text_interpreter", "cortex.agents.interpreters", "run_text_interpreter", "text_interpreter", 3),
    ("financial_interpreter", "cortex.agents.interpreters", "run_financial_interpreter", "financial_interpreter", 4),
    ("spatial_connector", "cortex.agents.connectors", "run_spatial_connector", "spatial_connector", 3),
    ("narrative_connector", "cortex.agents.connectors", "run_narrative_connector", "narrative_connector", 4),
    ("statistical_connector", "cortex.agents.connectors", "run_statistical_connector", "statistical_connector", 6),
    ("causal_chain_connector", "cortex.agents.connectors", "run_causal_chain_connector", "causal_chain_connector", 5),
    ("brain", "cortex.agents.brain", "run_brain", "brain", 1),
    ("validator", "cortex.agents.validator", "run_validator", "validator", 5),
    ("explorer_spawner", "cortex.agents.explorers", "run_explorer_spawner", "explorer_spawner", 4),
    ("curiosity_engine", "cortex.agents.curiosity", "run_curiosity_engine", "curiosity_engine", 3),
    ("web_searcher", "cortex.agents.web_searcher", "run_web_searcher", "web_searcher", 3),
    ("chronicler", "cortex.agents.chronicler", "run_chronicler", "chronicler", 5),
    ("discovery_engine", "cortex.agents.discovery", "run_discovery_engine", "discovery_engine", 2),
]


def check_env_requirements(func_name: str, required_vars: list[str]) -> tuple[bool, list[str]]:
    """Check if all required env vars are set. Returns (ok, missing)."""
    missing = [v for v in required_vars if not os.environ.get(v, "").strip()]
    return (len(missing) == 0, missing)
