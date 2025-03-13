import pytest

from plastron.ocr.core import XYWH, BBox, Scale


def test_xywh_to_bbox():
    xywh = XYWH(x=10, y=25, w=100, h=20)
    bbox = BBox.from_xywh(xywh)
    assert bbox.x1 == 10
    assert bbox.x2 == 110
    assert bbox.y1 == 25
    assert bbox.y2 == 45


def test_bbox_to_xywh():
    bbox = BBox(x1=50, x2=400, y1=10, y2=70)
    xywh = XYWH.from_bbox(bbox)
    assert xywh.x == 50
    assert xywh.y == 10
    assert xywh.w == 350
    assert xywh.h == 60


@pytest.mark.parametrize(
    ('region', 'expected_string'),
    [
        (XYWH(x=10, y=25, w=100, h=20), '10,25,100,20'),
        (BBox(x1=50, y1=10, x2=400, y2=70), '50,10,400,70'),
    ]
)
def test_stringify(region, expected_string):
    assert str(region) == expected_string


@pytest.mark.parametrize(
    ('unit', 'image_resolution', 'expected_scale'),
    [
        ('inch1200', (400, 400), (1 / 3, 1 / 3)),
        ('mm10', (508, 508), (2.0, 2.0)),
        ('pixel', (300, 300), (1, 1)),
        ('inch1200', (400, 400), Scale(1 / 3, 1 / 3)),
        ('mm10', (508, 508), Scale(2.0, 2.0)),
        ('pixel', (300, 300), Scale(1, 1)),
    ]
)
def test_get_scale(unit, image_resolution, expected_scale):
    assert Scale.from_resolution(image_resolution, unit) == expected_scale


def test_unknown_measurement_unit():
    with pytest.raises(ValueError):
        Scale.from_resolution((400, 400), 'BAD_UNIT')
