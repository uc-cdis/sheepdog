import pytest
from sheepdog.utils.transforms.bcr_xml_to_json import BcrXmlToJsonParser


def test_gdc_type_mappings(pg_driver):

       parser = BcrXmlToJsonParser("SAMPLE", psqlgraph=pg_driver)

       # parse float
       float_val = parser.map_to_gdc_types("float", "38.3")
       self.assertTrue(type(float_val), float)
       self.assertRaises(ValueError, parser.map_to_gdc_types, "float", "38k")

       # parse int
       int_val = parser.map_to_gdc_types("int", "38")
       self.assertTrue(type(int_val), int)
       self.assertRaises(ValueError, parser.map_to_gdc_types, "int", "4.9")

       # parse long
       long_val = parser.map_to_gdc_types("long", "3111118")
       self.assertTrue(type(long_val), long)
       self.assertRaises(ValueError, parser.map_to_gdc_types, "long", "4.9")

       # parse str
       str_val = parser.map_to_gdc_types("str", "Dummy Text")
       self.assertTrue(type(str_val), str)
       self.assertEqual(str_val, "Dummy Text")

       # parse str lower
       lower_val = parser.map_to_gdc_types("str.lower", "Dummy Text")
       self.assertTrue(type(lower_val), str)
       self.assertEqual(lower_val, "dummy text")

       # parse bool
       self.assertTrue(parser.map_to_gdc_types("bool", "yes"))
       self.assertTrue(parser.map_to_gdc_types("bool", "true"))
       self.assertFalse(parser.map_to_gdc_types("bool", "no"))
       self.assertFalse(parser.map_to_gdc_types("bool", "false"))

       self.assertRaises(ValueError, parser.map_to_gdc_types, "bool", "NAY")

