__author__ = 'mwas'

import unittest
from NotFound import not_found


class TestNotFound(unittest.TestCase):

    def test_common_use_case(self):
        # example dict
        example = dict(name="mwas", age=22, education=dict(primary="kayole one primary school", high_school="Ikawa high school", campus="karatina university",
                                                           grades=dict(high_school="B+", primary=312, campus="unfinished")))

        # test that it does not change the actual implementation of dict get method
        self.assertEqual('mwas', example.get('name', not_found))

        self.assertEqual(None, example.get('key_not_in_dict'), "{0} should be None".format(not_found))

        self.assertEqual("Ikawa high school", example.get('education', not_found).get('high_school'))

        self.assertEqual("B+", example.get('education', not_found).get('grades', not_found).get('high_school'))

        # test is does get method multiple times without raising errors
        self.assertEqual(None, example.get('mwaside'))
        self.assertEqual(None, example.get('education', not_found).get('secodary_school'))
        self.assertEqual(None, example.get('education', not_found).get('grades', not_found).get('secodary_school'))
        self.assertEqual(None, example.get('ewewe', not_found).get('grades', not_found).get('secodary_schoo'))


if __name__ == '__main__':
    unittest.main()
