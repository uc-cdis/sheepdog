from sheepdog.utils.transforms.bcr_xml_to_json import munge_property


def test_gdc_type_mappings():

       # parse float
       float_val = munge_property("38.3", "float")
       self.assertTrue(type(float_val), float)
       self.assertRaises(ValueError, munge_property, "38k", "float")

       # parse int
       int_val = munge_property("38", "int")
       self.assertTrue(type(int_val), int)
       self.assertRaises(ValueError, munge_property, "4.9", "int")

       # parse long
       long_val = munge_property("3111118", "long")
       self.assertTrue(type(long_val), long)
       self.assertRaises(ValueError, munge_property, "4.9", "long")

       # parse str
       str_val = munge_property("Dummy Text", "str")
       self.assertTrue(type(str_val), str)
       self.assertEqual(str_val, "Dummy Text")

       # parse str lower
       lower_val = munge_property("Dummy Text", "str.lower")
       self.assertTrue(type(lower_val), str)
       self.assertEqual(lower_val, "dummy text")

       # parse bool
       self.assertTrue(munge_property("yes", "bool"))
       self.assertTrue(munge_property("true", "bool"))
       self.assertFalse(munge_property("no", "bool"))
       self.assertFalse(munge_property("false", "bool"))

       self.assertRaises(ValueError, munge_property, "NAY", "bool")

