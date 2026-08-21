import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket):
    header = socket.recv(4)
    data_len = int.from_bytes(header, byteorder="big")

    data = b""
    while len(data) < data_len:
        part = socket.recv(data_len - len(data))
        data += part

    return data


def send_all(socket: socket.socket, bytes):
    data_len = len(bytes)

    header = data_len.to_bytes(4, byteorder="big")
    msg = header + bytes

    socket.send(msg)
