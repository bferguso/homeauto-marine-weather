"""Import marine standard forecast zone data from the Government of Canada OGC API
into the marine_forecast_zone Arches resource model.
"""

import json
import logging
import urllib.request
from urllib.error import URLError

from django.contrib.gis.geos import GEOSGeometry

from arches.app.models.models import (
    GraphModel,
    ResourceInstance,
    ResourceInstanceLifecycleState,
)
from arches_querysets.models import ResourceTileTree

logger = logging.getLogger(__name__)

GRAPH_SLUG = "forecast_zone"
SOURCE_URL = (
    "https://api.weather.gc.ca/collections/marine-standard-forecast-zones/items"
    "?lang=en&limit=100&offset=0&PROVINCE_C=BC&f=json"
)

# Feature property keys in the API response
PROP_OBJECT_ID = "OBJECTID"
PROP_CLC = "CLC"
PROP_FEATURE_ID = "FEATURE_ID"
PROP_NAME = "NAME"
PROP_KIND = "KIND"
PROP_USAGE = "USAGE"
PROP_DEPICTION = "DEPICTN"
PROP_PROVINCE_CODE = "PROVINCE_C"
PROP_WATERBODY_CODE = "WATRBODY_C"


class MarineForecastZoneImporter:
    """Creates or updates ``marine_forecast_zone`` resources from the Government of
    Canada marine standard forecast zones OGC API Features endpoint.

    Uses ``feature_id`` as the stable identifier for upsert: a resource is updated
    if one already exists with a matching value, otherwise a new resource is created.

    Usage::

        importer = MarineForecastZoneImporter()
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
                f"Failed to fetch marine forecast zone data from {self.url}: {exc}"
            ) from exc
        return data.get("features", [])

    def sync(self):
        """Upsert marine_forecast_zone resources for each feature from the API.

        Returns a ``(created, updated)`` tuple of counts.
        """
        features = self.fetch_features()
        if not features:
            logger.warning("No features returned from %s.", self.url)
            return 0, 0

        existing_by_feature_id = self._load_existing()

        created = updated = 0
        for feature in features:
            props = feature.get("properties") or {}
            feature_id = props.get(PROP_FEATURE_ID)

            if feature_id and feature_id in existing_by_feature_id:
                self._update_resource(existing_by_feature_id[feature_id], feature)
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
        """Return a dict of {feature_id: ResourceTileTree} for all existing resources."""
        existing = {}
        for resource in ResourceTileTree.get_tiles(GRAPH_SLUG):
            ids_tile = resource.aliased_data.zone_identifiers
            if ids_tile:
                feature_id = ids_tile.aliased_data.feature_id
                if feature_id:
                    existing[feature_id] = resource
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
        resource.append_tile("zone_identifiers")
        resource.append_tile("zone_details")
        resource.append_tile("zone_boundary")
        self._apply_values(resource, feature)
        resource.save(force_admin=True)

    def _update_resource(self, resource, feature):
        self._apply_values(resource, feature)
        resource.save(force_admin=True)

    def _apply_values(self, resource, feature):
        props = feature.get("properties") or {}
        geometry = feature.get("geometry")

        ids_tile = resource.aliased_data.zone_identifiers
        if ids_tile:
            ids = ids_tile.aliased_data
            zone_name = props.get(PROP_NAME) or ""
            ids.zone_name = {"en": {"value": zone_name, "direction": "ltr"}}
            ids.clc = props.get(PROP_CLC)
            ids.object_id = props.get(PROP_OBJECT_ID)
            ids.feature_id = props.get(PROP_FEATURE_ID)

        details_tile = resource.aliased_data.zone_details
        if details_tile:
            details = details_tile.aliased_data
            details.waterbody_code = props.get(PROP_WATERBODY_CODE)
            details.zone_kind_ = props.get(PROP_KIND)
            details.zone_usage = props.get(PROP_USAGE)
            details.zone_depiction = props.get(PROP_DEPICTION)
            details.province_code = props.get(PROP_PROVINCE_CODE)

        boundary_tile = resource.aliased_data.zone_boundary
        if boundary_tile and geometry:
            boundary_tile.aliased_data.zone_boundary = {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": self._valid_geometry(geometry), "properties": {}}
                ],
            }

    @staticmethod
    def _valid_geometry(geometry):
        """Return a valid version of the geometry dict, repairing self-intersections."""
        geos = GEOSGeometry(json.dumps(geometry))
        if not geos.valid:
            geos = geos.make_valid()
        return json.loads(geos.geojson)
