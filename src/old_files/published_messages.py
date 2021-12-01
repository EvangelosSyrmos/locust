import typing

class PublishedMessages(typing.NamedTuple):
    """Stores metadata about outgoing published messages."""

    qos: int
    topic: str
    start_time: float
    payload_size: int