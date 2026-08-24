import socket
import logger
import safe_socket
import os
import threading
from lottery import Lottery, Bet

class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.finalized_agencies = set()
        self.quorum_cond = threading.Condition()

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        client_bets = []
        agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = safe_socket.recv_all(
                    client_socket
                )
                if not client_message:
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    return
                if client_message == b"END":
                    break

                decoded_msg = client_message.decode("utf-8")
                decoded_bets = decoded_msg.strip().split("\n")
                
                for bet_str in decoded_bets:
                    if bet_str == "":
                        continue
                    bet = self._decode_bet(bet_str)
                    agency_id = bet.agency_id
                    client_bets.append(bet)
                    message_amount += 1

                safe_socket.send_all(client_socket, b"SUCCESS\n")

            if agency_id is not None:
                with self.quorum_cond:
                    self.finalized_agencies.add(agency_id)
                    
                    while len(self.finalized_agencies) < self.agency_quorum_min:
                        self.quorum_cond.wait()

                    self.quorum_cond.notify_all()

                lottery_storage_path = f"./bets-{agency_id}.csv"

                if os.path.exists(lottery_storage_path):
                    os.remove(lottery_storage_path)
                
                client_lottery = Lottery(lottery_storage_path)
                client_lottery.store_bets(client_bets)
                winners = self._determine_winners(client_lottery, agency_id)
                encoded_winners = self._encode_winners(winners)
                safe_socket.send_all(client_socket, encoded_winners)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def _decode_bet(self, bet_str: str) -> Bet:
        parts = bet_str.strip().split(",")
        bet = Bet(
            agency_id=int(parts[0]),
            first_name=parts[1],
            last_name=parts[2],
            document=int(parts[3]),
            birthdate=parts[4],
            number=int(parts[5]),
        )
        return bet

    def _determine_winners(self, lottery: Lottery, agency_id: int) -> list[Bet]:
        winners = []
        for bet in lottery.load_bets():
            if bet.agency_id == agency_id and lottery.has_won(bet):
                winners.append(bet)
        return winners

    def _encode_winners(self, winners: list[Bet]) -> bytes:
        lines = []
        for winner in winners:
            line = f"{winner.first_name},{winner.last_name},{winner.document},{winner.birthdate},{winner.number}"
            lines.append(line)
        return "\n".join(lines).encode("utf-8")

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,)
                )
                client_thread.start()
