import socket


def recv_all(socket_obj: socket.socket, amount: int) -> bytes:
    data = bytearray()
    while len(data) < amount:
        needed = amount - len(data)
        part = socket_obj.recv(needed)
        if not part:
            break
        data.extend(part)
    return bytes(data)


def send_all(socket_obj: socket.socket, data: bytes) -> None:
    total_sent = 0
    while total_sent < len(data):
        sent = socket_obj.send(data[total_sent:])
        if sent == 0:
            continue
        total_sent += sent
