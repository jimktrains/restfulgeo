import json

class NotFound(Exception):
    def __init__(self, klass, args):
        self.klass = str(klass)
        self.args = json.dumps(args)
        s = ("%s(%s)" % (self.klass, args) )
        print(s)
    def __str__(self):
        s = ("%s(%s)" % (self.klass, args) )
        print(s)
        return s
