class Student:

    def __init__(self):
        self.name = "Rohit"
        self.id = 101

    def get_name(self):
        return self.name

    def get_id(self):
        return self.id

    def __str__(self):
        return "Student[id = %d, name = %s]".format(self.id, self.name)
