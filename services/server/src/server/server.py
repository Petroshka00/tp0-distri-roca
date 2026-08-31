import socket
import logger
import safe_socket
import os
import threading
import signal
from lottery import Lottery, Bet

class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.finalized_agencies = set()
        self.quorum_cond = threading.Condition()
        self.running = True
        self.server_socket = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        self.client_sockets = set()
        self.client_threads = []
        self.clients_lock = threading.Lock()

        lottery_storage_path = "./bets.csv"
        if os.path.exists(lottery_storage_path):
            os.remove(lottery_storage_path)
        self.lottery = Lottery(lottery_storage_path)
        self.lottery_lock = threading.Lock()

    def _register_client(self, client_socket, client_thread):
        with self.clients_lock:
            self.client_sockets.add(client_socket)
            self.client_threads.append(client_thread)

    def _unregister_client(self, client_socket):
        with self.clients_lock:
            self.client_sockets.remove(client_socket)

    def _process_bet_batch(self, batch_msg: str):
        decoded_bets = batch_msg.strip().split("\n")
        batch_bets = []
        agency_id = None

        for bet_str in decoded_bets:
            if not bet_str:
                continue
            bet = self._decode_bet(bet_str)
            agency_id = bet.agency_id
            batch_bets.append(bet)

        if batch_bets:
            with self.lottery_lock:
                self.lottery.store_bets(batch_bets)

        return agency_id, len(batch_bets)

    def _receive_client_bets(self, client_socket):
        message_amount = 0
        agency_id = None

        client_message = safe_socket.recv_all(client_socket)
        while client_message and client_message != b"END":
            decoded_msg = client_message.decode("utf-8")
            current_agency, bets_count = self._process_bet_batch(decoded_msg)
            if current_agency is not None:
                agency_id = current_agency
            message_amount += bets_count

            safe_socket.send_msg(client_socket, b"SUCCESS\n")
            client_message = safe_socket.recv_all(client_socket)

        completed = (client_message == b"END")
        return agency_id, message_amount, completed

    def _wait_for_quorum(self, agency_id: int) -> bool:
        with self.quorum_cond:
            self.finalized_agencies.add(agency_id)
            while len(self.finalized_agencies) < self.agency_quorum_min and self.running:
                self.quorum_cond.wait()
            self.quorum_cond.notify_all()
        return self.running

    def _process_lottery_results(self, agency_id: int) -> list[Bet]:
        return self._determine_winners(agency_id)

    def _send_winners(self, client_socket, winners: list[Bet]) -> None:
        encoded_winners = self._encode_winners(winners)
        safe_socket.send_msg(client_socket, encoded_winners)

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)

            agency_id, message_amount, completed = self._receive_client_bets(client_socket)
            if not completed or agency_id is None:
                logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                return

            if not self._wait_for_quorum(agency_id):
                return

            winners = self._process_lottery_results(agency_id)
            self._send_winners(client_socket, winners)
            logger.info(action, logger.LogResult.success, "messages-amount", message_amount)

        except Exception as e:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
            raise e
        finally:
            self._unregister_client(client_socket)
            try:
                client_socket.close()
            except Exception:
                pass

    def _decode_bet(self, bet_str: str) -> Bet:
        parts = bet_str.strip().split(",")
        return Bet(
            agency_id=int(parts[0]),
            first_name=parts[1],
            last_name=parts[2],
            document=int(parts[3]),
            birthdate=parts[4],
            number=int(parts[5]),
        )

    def _determine_winners(self, agency_id: int) -> list[Bet]:
        with self.lottery_lock:
            return [
                bet for bet in self.lottery.load_bets()
                if bet.agency_id == agency_id and self.lottery.has_won(bet)
            ]

    def _encode_winners(self, winners: list[Bet]) -> bytes:
        lines = [
            f"{winner.first_name},{winner.last_name},{winner.document},{winner.birthdate},{winner.number}"
            for winner in winners
        ]
        return "\n".join(lines).encode("utf-8")

    def _handle_sigterm(self, signum, frame):
        logger.info("sigterm_handler", logger.LogResult.in_progress)
        self.running = False
        with self.quorum_cond:
            self.quorum_cond.notify_all()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

        with self.clients_lock:
            for socket in list(self.client_sockets):
                try:
                    socket.shutdown(socket.SHUT_RDWR)
                    socket.close()
                except Exception:
                    pass
            self.client_sockets.clear()

    def run(self):
        action = "accept-connection"
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.server_host, self.server_port))
        server_socket.listen()
        self.server_socket = server_socket

        while self.running:
            try:
                logger.info(action, logger.LogResult.in_progress)
                client_socket, _ = server_socket.accept()
            except Exception as e:
                if not self.running:
                    break
                logger.error(action, logger.LogResult.fail)
                raise e

            logger.info(action, logger.LogResult.success)
            client_thread = threading.Thread(
                target=self._handle_client,
                args=(client_socket,)
            )
            self._register_client(client_socket, client_thread)
            client_thread.start()

        with self.clients_lock:
            threads = list(self.client_threads)
        for t in threads:
            t.join(timeout=0.5)

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

