import safe_socket
import logger
from lottery import Bet

HEADER_SIZE = 5

CONNECT = 1
BET_BATCH = 2
END = 3
ACK = 4
WINNERS = 5
ERROR = 6


def send_msg(socket_obj, opcode: int, payload: bytes = b"") -> None:
    header = bytes([opcode]) + len(payload).to_bytes(4, byteorder="big")
    safe_socket.send_all(socket_obj, header + payload)


def recv_msg(socket_obj) -> tuple[int | None, bytes]:
    header = safe_socket.recv_all(socket_obj, HEADER_SIZE)
    if not header or len(header) < HEADER_SIZE:
        return None, b""
    opcode = header[0]
    data_len = int.from_bytes(header[1:5], byteorder="big")
    if data_len == 0:
        return opcode, b""
    payload = safe_socket.recv_all(socket_obj, data_len)
    if len(payload) < data_len:
        return None, b""
    return opcode, payload


def decode_bet(bet_str: str, agency_id: int) -> Bet | None:
    parts = bet_str.strip().split(",")
    if len(parts) == 5:
        try:
            return Bet(
                agency_id=agency_id,
                first_name=parts[0],
                last_name=parts[1],
                document=int(parts[2]),
                birthdate=parts[3],
                number=int(parts[4]),
            )
        except (ValueError, IndexError) as e:
            logger.error("decode-bet", logger.LogResult.fail, "error", str(e), "bet", bet_str)
            return None
    logger.error("decode-bet", logger.LogResult.fail, "error", "Invalid field count", "bet", bet_str)
    return None


def decode_batch(payload: bytes, agency_id: int) -> list[Bet]:
    decoded_bets = payload.decode("utf-8").strip().split("\n")
    batch_bets = []
    for bet_str in decoded_bets:
        if not bet_str:
            continue
        bet = decode_bet(bet_str, agency_id)
        if bet is not None:
            batch_bets.append(bet)
    return batch_bets


def encode_winners(winners: list[Bet]) -> bytes:
    lines = [
        f"{winner.first_name},{winner.last_name},{winner.document},{winner.birthdate},{winner.number}"
        for winner in winners
    ]
    return "\n".join(lines).encode("utf-8")
