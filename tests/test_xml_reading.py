from untangle import Element

from transx2gtfs.dataio import (
    get_xml_paths,
    read_unpacked_xml,
    read_xml_inside_nested_zip,
    read_xml_inside_zip,
)


def test_reading_from_unpacked_directory(data_dir):
    xml_paths = get_xml_paths(data_dir)
    assert len(xml_paths) == 3

    for path in xml_paths:
        assert isinstance(path, str)
        assert path.endswith(".xml")

        data, filesize, name = read_unpacked_xml(path)
        assert isinstance(data, Element)
        assert "TransXChange" in data.__dir__()
        assert filesize > 0
        assert name.endswith(".xml")


def test_reading_from_packed(packed_zip):
    xml_paths = get_xml_paths(packed_zip)
    assert len(xml_paths) == 3

    for path in xml_paths:
        assert isinstance(path, dict)
        # {"transxchange.xml": "/path/to/packed.zip"}
        ((xml_name, zip_path),) = path.items()
        assert xml_name.endswith(".xml")
        assert zip_path.endswith(".zip")

        data, filesize, name = read_xml_inside_zip(path)
        assert isinstance(data, Element)
        assert filesize > 0
        assert "TransXChange" in data.__dir__()
        assert name == xml_name


def test_reading_from_nested(nested_zip):
    xml_paths = get_xml_paths(nested_zip)
    assert len(xml_paths) == 3

    for path in xml_paths:
        assert isinstance(path, dict)
        # {"outer.zip": {"inner.zip": "transxchange.xml"}}
        ((zip_path, inner),) = path.items()
        assert zip_path.endswith(".zip")
        assert isinstance(inner, dict)

        data, filesize, name = read_xml_inside_nested_zip(path)
        assert isinstance(data, Element)
        assert filesize > 0
        assert "TransXChange" in data.__dir__()
        assert name.endswith(".xml")


def test_reading_from_directory_with_zip(dir_with_packed):
    xml_paths = get_xml_paths(dir_with_packed)
    assert len(xml_paths) == 3
    assert all(isinstance(path, dict) for path in xml_paths)
