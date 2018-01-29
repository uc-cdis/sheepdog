from sheepdog.utils.transforms.bcr_xml_to_json import munge_property


def test_gdc_type_mappings():

       # parse float
       float_val = munge_property("float", "38.3")
       self.assertTrue(type(float_val), float)
       self.assertRaises(ValueError, munge_property, "float", "38k")

       # parse int
       int_val = munge_property("int", "38")
       self.assertTrue(type(int_val), int)
       self.assertRaises(ValueError, munge_property, "int", "4.9")

       # parse long
       long_val = munge_property("long", "3111118")
       self.assertTrue(type(long_val), long)
       self.assertRaises(ValueError, munge_property, "long", "4.9")

       # parse str
       str_val = munge_property("str", "Dummy Text")
       self.assertTrue(type(str_val), str)
       self.assertEqual(str_val, "Dummy Text")

       # parse str lower
       lower_val = munge_property("str.lower", "Dummy Text")
       self.assertTrue(type(lower_val), str)
       self.assertEqual(lower_val, "dummy text")

       # parse bool
       self.assertTrue(munge_property("bool", "yes"))
       self.assertTrue(munge_property("bool", "true"))
       self.assertFalse(munge_property("bool", "no"))
       self.assertFalse(munge_property("bool", "false"))

       self.assertRaises(ValueError, munge_property, "bool", "NAY")

