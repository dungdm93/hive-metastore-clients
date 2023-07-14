from types import TracebackType
from typing import Self
from urllib.parse import urlparse

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

from .internal.ThriftHiveMetastore import Client


class ThriftHiveMetastore:
    _transport: TTransport
    _client: Client

    def __init__(self, uri: str):
        url_parts = urlparse(uri)
        transport = TSocket.TSocket(url_parts.hostname, url_parts.port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self._client = Client(protocol)

    def __enter__(self) -> Self:
        self._transport.open()
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None
    ) -> None:
        self._transport.close()
