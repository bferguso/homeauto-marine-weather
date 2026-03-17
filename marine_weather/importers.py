"""Import weather station data from the Government of Canada SWOB marine stations API
into the weather_station Arches resource model.
"""

import json
import logging
import urllib.request
from urllib.error import URLError

from arches.app.models.models import (
    GraphModel,
    ResourceInstance,
    ResourceInstanceLifecycleState,
)
from arches_querysets.models import ResourceTileTree

logger = logging.getLogger(__name__)

GRAPH_SLUG = "weather_station"
SOURCE_URL = (
    "https://api.weather.gc.ca/collections/swob-marine-stations/items"
    "?f=json&bbox=-139,48,-123,55"
)

# Feature property keys in the API response
PROP_MSC_ID = "msc_id"
PROP_WMO_ID = "wmo_id"
PROP_ICAO_ID = "icao_id"
PROP_IATA_ID = "iata_id"
PROP_STATION_NAME = "name_en"
PROP_AUTO_STATION = "auto_man"
PROP_DATA_PROVIDER = "data_provider"


class WeatherStationImporter:
    """Creates or updates ``weather_station`` resources from the Government of
    Canada SWOB marine stations OGC API Features endpoint.

    Uses ``msc_id`` as the stable identifier for upsert: a resource is updated
    if one already exists with a matching value, otherwise a new resource is
    created.

    Usage::

        importer = WeatherStationImporter()
        created, updated = importer.sync()
    """

    def __init__(self, url=SOURCE_URL):
        self.url = url
        self._graph = None
        self._initial_lifecycle_state = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_features(self):
        """Return the list of GeoJSON features from the source URL."""
        try:
            with urllib.request.urlopen(self.url) as response:
                data = json.loads(response.read())
        except (URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Failed to fetch weather station data from {self.url}: {exc}"
            ) from exc
        return data.get("features", [])

    def sync(self):
        """Upsert weather_station resources for each feature from the API.

        Returns a ``(created, updated)`` tuple of counts.
        """
        features = self.fetch_features()
        if not features:
            logger.warning("No features returned from %s.", self.url)
            return 0, 0

        existing_by_msc_id = self._load_existing()

        created = updated = 0
        for feature in features:
            props = feature.get("properties") or {}
            msc_id = props.get(PROP_MSC_ID)

            if msc_id and msc_id in existing_by_msc_id:
                self._update_resource(existing_by_msc_id[msc_id], feature)
                updated += 1
            else:
                self._create_resource(feature)
                created += 1

        logger.info("Sync complete: %d created, %d updated.", created, updated)
        return created, updated

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_existing(self):
        """Return a dict of {msc_id: ResourceTileTree} for all existing resources."""
        existing = {}
        for resource in ResourceTileTree.get_tiles(GRAPH_SLUG):
            ids_tile = resource.aliased_data.station_identifiers
            if ids_tile:
                msc_id = ids_tile.aliased_data.msc_id
                if msc_id:
                    existing[msc_id] = resource
        return existing

    def _graph_obj(self):
        if self._graph is None:
            self._graph = GraphModel.objects.select_related(
                "publication", "resource_instance_lifecycle"
            ).get(slug=GRAPH_SLUG)
        return self._graph

    def _lifecycle_state(self):
        if self._initial_lifecycle_state is None:
            self._initial_lifecycle_state = (
                ResourceInstanceLifecycleState.objects.get(
                    resource_instance_lifecycle=self._graph_obj().resource_instance_lifecycle,
                    is_initial_state=True,
                )
            )
        return self._initial_lifecycle_state

    def _create_resource(self, feature):
        graph = self._graph_obj()
        new_instance = ResourceInstance.objects.create(
            graph=graph,
            graph_publication_id=graph.publication_id,
            resource_instance_lifecycle_state=self._lifecycle_state(),
        )
        resource = ResourceTileTree.get_tiles(GRAPH_SLUG).get(pk=new_instance.pk)
        resource.append_tile("station_identifiers")
        resource.append_tile("station_details")
        resource.append_tile("station_location")
        self._apply_values(resource, feature)
        resource.save(force_admin=True)

    def _update_resource(self, resource, feature):
        self._apply_values(resource, feature)
        resource.save(force_admin=True)

    def _apply_values(self, resource, feature):
        props = feature.get("properties") or {}
        geometry = feature.get("geometry")
        feature_id = feature.get("id")

        ids_tile = resource.aliased_data.station_identifiers
        if ids_tile:
            ids = ids_tile.aliased_data
            ids.msc_id = props.get(PROP_MSC_ID)
            ids.wmo_id = props.get(PROP_WMO_ID)
            ids.icao_id = props.get(PROP_ICAO_ID) or None
            ids.iata_id = props.get(PROP_IATA_ID) or None
            station_name = props.get(PROP_STATION_NAME) or ""
            ids.station_name = {"en": {"value": station_name, "direction": "ltr"}}
        details_tile = resource.aliased_data.station_details
        if details_tile:
            details = details_tile.aliased_data
            raw_auto = props.get(PROP_AUTO_STATION)
            details.automatic_station = True if raw_auto == 'AUTO' else False
            provider = props.get(PROP_DATA_PROVIDER) or ""
            details.data_provider = {"en": {"value": provider, "direction": "ltr"}}

        loc_tile = resource.aliased_data.station_location
        if loc_tile and geometry:
            loc = loc_tile.aliased_data
            loc.station_location = {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": geometry, "properties": {}}
                ],
            }
            if feature_id is not None:
                loc.feature_id = int(feature_id)
