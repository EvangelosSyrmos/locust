import typing

class SubscribedMessages(typing.NamedTuple):
    """Stores metadata about ingoing published messages."""

    qos: int
    topic: str
    start_time: float