import socket


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


def recv_all(socket_obj: socket.socket, amount: int) -> bytes:
    return _recv_exact(socket_obj, amount)


def send_all(socket_obj: socket.socket, data: bytes) -> None:
    _send_exact(socket_obj, data)



