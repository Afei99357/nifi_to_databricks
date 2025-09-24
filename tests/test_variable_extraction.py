import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT_DIR)

from tools.variable_extraction import find_variable_definitions


def test_extract_text_dynamic_property_detected_as_definition():
    processors = [
        {
            "id": "proc-1",
            "name": 'Content to parameter "filename"',
            "type": "org.apache.nifi.processors.standard.ExtractText",
            "properties": {
                "Character Set": "UTF-8",
                "aggr_filename": "(.*)",
            },
            "parentGroupName": "Determine dependent aggregations",
        }
    ]

    definitions = find_variable_definitions(processors)

    assert "aggr_filename" in definitions
    entry = definitions["aggr_filename"][0]
    assert entry["processor_id"] == "proc-1"
    assert entry["definition_type"] == "static"
