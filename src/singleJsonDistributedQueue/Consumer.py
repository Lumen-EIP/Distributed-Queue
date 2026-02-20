from uuid import UUID, uuid1


class Consumer:
    def __init__(self):
        self.consumerId: UUID = uuid1()
        pass

    def writeRequest(self):
        pass

    def readRequest(self):
        pass

    def processTask(self):
        pass

    def aliveAcknowledgement(self):
        pass

    def checkBrokerAvailablity(self):
        pass
