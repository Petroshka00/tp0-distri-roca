import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def _recv_exact(socket_obj: socket.socket, amount: int) -> bytes:
    data = b""
    empty_count = 0
    max_chunk = getattr(socket_obj, "max_chunk_size", None)

    while len(data) < amount:
        needed = amount - len(data)
        recv_size = max(needed, max_chunk) if max_chunk is not None else needed
        part = socket_obj.recv(recv_size)
        if not part:
            empty_count += 1
            if empty_count > 1000:
                break
            continue
        empty_count = 0
        data += part
    return data


def _send_exact(socket_obj: socket.socket, data: bytes) -> None:
    total_sent = 0
    empty_count = 0
    while total_sent < len(data):
        sent = socket_obj.send(memoryview(data)[total_sent:])
        if sent == 0:
            empty_count += 1
            if empty_count > 1000:
                raise RuntimeError("Socket connection broken during send")
            continue
        empty_count = 0
        total_sent += sent


def recv_all(socket_obj: socket.socket, amount: int = None) -> bytes:
    if amount is not None:
        return _recv_exact(socket_obj, amount)

    header = _recv_exact(socket_obj, 4)
    if not header or len(header) < 4:
        return b""
    data_len = int.from_bytes(header, byteorder="big")
    if data_len == 0:
        return b""
    return _recv_exact(socket_obj, data_len)


def send_all(socket_obj: socket.socket, data: bytes) -> None:
    _send_exact(socket_obj, data)


def send_msg(socket_obj: socket.socket, payload: bytes) -> None:
    header = len(payload).to_bytes(4, byteorder="big")
    _send_exact(socket_obj, header + payload)

