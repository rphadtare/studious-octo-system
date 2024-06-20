import unittest

from helpers.my_unit_test.Student import Student


class StudentTestMethods(unittest.TestCase):

    def test_Student(self):
        student_local = Student()
        self.assertEqual(101, student_local.id)
        self.assertEqual("Rohit", student_local.name)


if __name__ == '__main__':
    unittest.main()
