import socket

HEADER_SIZE = 4


def _recv_exact(socket_obj: socket.socket, amount: int) -> bytes:
    data = bytearray()
    while len(data) < amount:
        needed = amount - len(data)
        part = socket_obj.recv(needed)
        if not part:
            break
        data.extend(part)
    return bytes(data)


def _send_exact(socket_obj: socket.socket, data: bytes) -> None:
    total_sent = 0
    while total_sent < len(data):
        sent = socket_obj.send(data[total_sent:])
        if sent == 0:
            continue
        total_sent += sent


def recv_all(socket_obj: socket.socket, amount: int | None = None) -> bytes:
    if amount is not None:
        return _recv_exact(socket_obj, amount)

    header = _recv_exact(socket_obj, HEADER_SIZE)
    if not header or len(header) < HEADER_SIZE:
        return b""
    data_len = int.from_bytes(header, byteorder="big")
    if data_len == 0:
        return b""
    payload = _recv_exact(socket_obj, data_len)
    return payload


def send_all(socket_obj: socket.socket, data: bytes) -> None:
    _send_exact(socket_obj, data)


def send_msg(socket_obj: socket.socket, payload: bytes) -> None:
    header = len(payload).to_bytes(HEADER_SIZE, byteorder="big")
    _send_exact(socket_obj, header + payload)

