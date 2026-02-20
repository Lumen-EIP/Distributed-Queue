from enum import StrEnum


class EventOwner(StrEnum):
    BROKER_MANAGER = "Broker Manager"
    PUBLISHER = "Publisher"
    CONSUMER = "Consumer"
