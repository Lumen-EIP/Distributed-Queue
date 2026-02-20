from enum import StrEnum


class EventType(StrEnum):
    WRITE = "Write In Queue"
    READ = "Read From Queue"
    SHUTDOWN = "Shutdown Yourself"
