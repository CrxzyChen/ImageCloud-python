from urllib import parse

import pymongo


class MongoDB:
    def __init__(self, username, password, host, port="27017"):
        self.username = parse.quote_plus(username)
        self.password = parse.quote_plus(password)
        self.host = host
        self.port = port
        self.uri = "mongodb://{0}:{1}@{2}:{3}".format(self.username, self.password, self.host, self.port)
        self.client = pymongo.MongoClient(self.uri)

    def __getattr__(self, item):
        return self.client.get_database(item)
