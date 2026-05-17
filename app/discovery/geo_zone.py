from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal


MOSCOW_REFERENCE_LAT = 55.7558
MOSCOW_REFERENCE_LON = 37.6176

GEO_BUCKET_OUTSIDE = "outside"
GEO_BUCKET_OUTER_BAND = "outer_band"
GEO_BUCKET_CORE = "core"

GeoBucket = Literal["outside", "outer_band", "core"]


@dataclass(frozen=True)
class GeoPoint:
    latitude_dd: float
    longitude_dd: float


@dataclass(frozen=True)
class GeoAnchor:
    name: str
    latitude_dd: float
    longitude_dd: float

    def as_point(self) -> GeoPoint:
        return GeoPoint(latitude_dd=self.latitude_dd, longitude_dd=self.longitude_dd)


@dataclass(frozen=True)
class GeoZoneDefinition:
    moscow_reference_point: GeoPoint
    outer_anchors: tuple[GeoAnchor, ...]
    inner_anchors: tuple[GeoAnchor, ...]
    outer_polygon_vertices: tuple[GeoPoint, ...]
    inner_polygon_vertices: tuple[GeoPoint, ...]
    outside_weight: int = 0
    outer_band_weight: int = 1
    core_weight: int = 5

    def __post_init__(self) -> None:
        _validate_polygon_definition(
            anchors=self.outer_anchors,
            vertices=self.outer_polygon_vertices,
            polygon_name="outer",
        )
        _validate_polygon_definition(
            anchors=self.inner_anchors,
            vertices=self.inner_polygon_vertices,
            polygon_name="inner",
        )


@dataclass(frozen=True)
class GeoZoneClassification:
    geo_bucket: GeoBucket
    geo_weight: int
    inside_outer_polygon: bool
    inside_inner_polygon: bool
    distance_to_moscow_km: float


MOSCOW_REFERENCE_POINT = GeoPoint(
    latitude_dd=MOSCOW_REFERENCE_LAT,
    longitude_dd=MOSCOW_REFERENCE_LON,
)

OUTER_ANCHORS = (
    GeoAnchor("Великие Луки", 56.3420, 30.5210),
    GeoAnchor("Вышний Волочек", 57.5913, 34.5645),
    GeoAnchor("Ярославль", 57.6261, 39.8845),
    GeoAnchor("Ковров", 56.3572, 41.3190),
    GeoAnchor("Сасово", 54.3537, 41.9197),
    GeoAnchor("Липецк", 52.6100, 39.5942),
    GeoAnchor("Брянск", 53.2436, 34.3640),
    GeoAnchor("Смоленск", 54.7826, 32.0453),
)

OUTER_POLYGON_VERTICES = (
    GeoPoint(56.3420, 30.5210),
    GeoPoint(57.5913, 34.5645),
    GeoPoint(57.6261, 39.8845),
    GeoPoint(56.3572, 41.3190),
    GeoPoint(54.3537, 41.9197),
    GeoPoint(52.6100, 39.5942),
    GeoPoint(53.2436, 34.3640),
    GeoPoint(54.7826, 32.0453),
)

INNER_ANCHORS = (
    GeoAnchor("Ржев", 56.2620, 34.3290),
    GeoAnchor("Тверь", 56.8587, 35.9176),
    GeoAnchor("Ростов Великий", 57.1914, 39.4139),
    GeoAnchor("Владимир", 56.1290, 40.4070),
    GeoAnchor("Рязань", 54.6291, 39.7364),
    GeoAnchor("Тула", 54.1930, 37.6178),
    GeoAnchor("Калуга", 54.5138, 36.2612),
    GeoAnchor("Вязьма", 55.2100, 34.2950),
)

INNER_POLYGON_VERTICES = (
    GeoPoint(56.2620, 34.3290),
    GeoPoint(56.8587, 35.9176),
    GeoPoint(57.1914, 39.4139),
    GeoPoint(56.1290, 40.4070),
    GeoPoint(54.6291, 39.7364),
    GeoPoint(54.1930, 37.6178),
    GeoPoint(54.5138, 36.2612),
    GeoPoint(55.2100, 34.2950),
)


def _validate_polygon_definition(
    *,
    anchors: tuple[GeoAnchor, ...],
    vertices: tuple[GeoPoint, ...],
    polygon_name: str,
) -> None:
    if len(vertices) < 3:
        raise ValueError(f"{polygon_name} polygon must contain at least three vertices")
    if len(anchors) != len(vertices):
        raise ValueError(f"{polygon_name} anchors and vertices must have identical lengths")
    for index, (anchor, vertex) in enumerate(zip(anchors, vertices), start=1):
        if not math.isclose(anchor.latitude_dd, vertex.latitude_dd, abs_tol=1e-9):
            raise ValueError(f"{polygon_name} anchor {index} latitude does not match vertex")
        if not math.isclose(anchor.longitude_dd, vertex.longitude_dd, abs_tol=1e-9):
            raise ValueError(f"{polygon_name} anchor {index} longitude does not match vertex")


DEFAULT_MOSCOW_GEO_ZONE = GeoZoneDefinition(
    moscow_reference_point=MOSCOW_REFERENCE_POINT,
    outer_anchors=OUTER_ANCHORS,
    inner_anchors=INNER_ANCHORS,
    outer_polygon_vertices=OUTER_POLYGON_VERTICES,
    inner_polygon_vertices=INNER_POLYGON_VERTICES,
)


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    return 2.0 * earth_radius_km * math.asin(math.sqrt(a))


def _point_on_segment(
    x: float,
    y: float,
    *,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    eps: float = 1e-9,
) -> bool:
    cross = (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)
    if abs(cross) > eps:
        return False
    dot = (x - x1) * (x2 - x1) + (y - y1) * (y2 - y1)
    if dot < -eps:
        return False
    squared_length = (x2 - x1) ** 2 + (y2 - y1) ** 2
    if dot - squared_length > eps:
        return False
    return True


def point_in_polygon(latitude_dd: float, longitude_dd: float, polygon: tuple[GeoPoint, ...]) -> bool:
    if len(polygon) < 3:
        return False
    x = longitude_dd
    y = latitude_dd
    inside = False
    for index, current in enumerate(polygon):
        nxt = polygon[(index + 1) % len(polygon)]
        x1 = current.longitude_dd
        y1 = current.latitude_dd
        x2 = nxt.longitude_dd
        y2 = nxt.latitude_dd
        if _point_on_segment(x, y, x1=x1, y1=y1, x2=x2, y2=y2):
            return True
        if (y1 > y) != (y2 > y):
            intersection_x = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < intersection_x:
                inside = not inside
    return inside


def classify(
    latitude_dd: float,
    longitude_dd: float,
    *,
    definition: GeoZoneDefinition = DEFAULT_MOSCOW_GEO_ZONE,
) -> GeoZoneClassification:
    inside_outer_polygon = point_in_polygon(
        latitude_dd,
        longitude_dd,
        definition.outer_polygon_vertices,
    )
    inside_inner_polygon = inside_outer_polygon and point_in_polygon(
        latitude_dd,
        longitude_dd,
        definition.inner_polygon_vertices,
    )

    if inside_inner_polygon:
        geo_bucket = GEO_BUCKET_CORE
        geo_weight = definition.core_weight
    elif inside_outer_polygon:
        geo_bucket = GEO_BUCKET_OUTER_BAND
        geo_weight = definition.outer_band_weight
    else:
        geo_bucket = GEO_BUCKET_OUTSIDE
        geo_weight = definition.outside_weight

    distance_to_moscow_km = haversine_distance_km(
        definition.moscow_reference_point.latitude_dd,
        definition.moscow_reference_point.longitude_dd,
        latitude_dd,
        longitude_dd,
    )
    return GeoZoneClassification(
        geo_bucket=geo_bucket,
        geo_weight=geo_weight,
        inside_outer_polygon=inside_outer_polygon,
        inside_inner_polygon=inside_inner_polygon,
        distance_to_moscow_km=distance_to_moscow_km,
    )


__all__ = [
    "DEFAULT_MOSCOW_GEO_ZONE",
    "GEO_BUCKET_CORE",
    "GEO_BUCKET_OUTER_BAND",
    "GEO_BUCKET_OUTSIDE",
    "GeoAnchor",
    "GeoBucket",
    "GeoPoint",
    "GeoZoneClassification",
    "GeoZoneDefinition",
    "INNER_ANCHORS",
    "INNER_POLYGON_VERTICES",
    "MOSCOW_REFERENCE_LAT",
    "MOSCOW_REFERENCE_LON",
    "MOSCOW_REFERENCE_POINT",
    "OUTER_ANCHORS",
    "OUTER_POLYGON_VERTICES",
    "classify",
    "haversine_distance_km",
    "point_in_polygon",
]
