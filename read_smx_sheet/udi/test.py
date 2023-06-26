class Name:
    def __init__(self, name):
        Name.NAME = self.func(name)
        self.use()
    def func(self, name):
        return name
    def use(self):
        print(Name.NAME)

omar = Name("omar")
# omar.use()
# print(omar.func())