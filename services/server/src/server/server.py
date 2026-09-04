import socket
import os
import threading
import signal
import logger
from . import protocol
from lottery import Lottery, Bet

DEFAULT_STORAGE_PATH = "./bets.csv"
THREAD_JOIN_TIMEOUT_SEC = 0.5


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.finalized_agencies = set()
        self.quorum_cond = threading.Condition()
        self.running = True
        self.server_socket: socket.socket | None = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        self.client_sockets = set()
        self.client_threads: list[threading.Thread] = []
        self.clients_lock = threading.RLock()

        if os.path.exists(DEFAULT_STORAGE_PATH):
            try:
                os.remove(DEFAULT_STORAGE_PATH)
            except OSError as e:
                logger.error("remove-storage-file", logger.LogResult.fail, "error", str(e))
        self.lottery = Lottery(DEFAULT_STORAGE_PATH)
        self.lottery_lock = threading.Lock()

    def _register_client(self, client_socket: socket.socket, client_thread: threading.Thread) -> None:
        with self.clients_lock:
            self.client_sockets.add(client_socket)
            self.client_threads.append(client_thread)

    def _unregister_client(self, client_socket: socket.socket) -> None:
        with self.clients_lock:
            self.client_sockets.discard(client_socket)

    def _wait_for_quorum(self, agency_id: int) -> bool:
        with self.quorum_cond:
            self.finalized_agencies.add(agency_id)
            while len(self.finalized_agencies) < self.agency_quorum_min and self.running:
                self.quorum_cond.wait()
            self.quorum_cond.notify_all()
        return self.running

    def _determine_winners(self, agency_id: int) -> list[Bet]:
        with self.lottery_lock:
            return [
                bet for bet in self.lottery.load_bets()
                if bet.agency_id == agency_id and self.lottery.has_won(bet)
            ]

    def _handle_client(self, client_socket: socket.socket) -> None:
        action = "handle-client"
        message_amount = 0
        try:
            logger.info(action, logger.LogResult.in_progress)

            opcode, payload = protocol.recv_msg(client_socket)
            if opcode != protocol.CONNECT:
                logger.error(action, logger.LogResult.fail, "error", f"Expected CONNECT, got {opcode}")
                protocol.send_msg(client_socket, protocol.ERROR, b"Expected CONNECT")
                return

            agency_id = int(payload.decode("utf-8").strip())
            protocol.send_msg(client_socket, protocol.ACK)

            completed = False
            while self.running:
                opcode, payload = protocol.recv_msg(client_socket)
                if opcode is None:
                    break

                if opcode == protocol.BET_BATCH:
                    bets = protocol.decode_batch(payload, agency_id)
                    if bets:
                        with self.lottery_lock:
                            self.lottery.store_bets(bets)
                    message_amount += len(bets)
                    protocol.send_msg(client_socket, protocol.ACK)

                elif opcode == protocol.END:
                    completed = True
                    break

                else:
                    logger.error(action, logger.LogResult.fail, "error", f"Unexpected opcode {opcode}")
                    protocol.send_msg(client_socket, protocol.ERROR, b"Unexpected opcode")
                    return

            if not completed:
                logger.error(action, logger.LogResult.fail, "messages-amount", message_amount)
                return

            if not self._wait_for_quorum(agency_id):
                return

            winners = self._determine_winners(agency_id)
            encoded_winners = protocol.encode_winners(winners)
            protocol.send_msg(client_socket, protocol.WINNERS, encoded_winners)
            logger.info(action, logger.LogResult.success, "messages-amount", message_amount)

        except Exception as e:
            logger.error(action, logger.LogResult.fail, "messages-amount", message_amount, "error", str(e))
        finally:
            self._unregister_client(client_socket)
            try:
                client_socket.close()
            except Exception as e:
                logger.error("close-client-socket", logger.LogResult.fail, "error", str(e))



    def _close_server_socket(self) -> None:
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logger.error("close-server-socket", logger.LogResult.fail, "error", str(e))

    def _handle_sigterm(self, signum: int, frame: object) -> None:
        logger.info("sigterm_handler", logger.LogResult.in_progress)
        self.running = False
        with self.quorum_cond:
            self.quorum_cond.notify_all()

        self._close_server_socket()

        with self.clients_lock:
            for sock in list(self.client_sockets):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except Exception as e:
                    logger.error("shutdown-client-socket", logger.LogResult.fail, "error", str(e))
            self.client_sockets.clear()

    def run(self) -> None:
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
                if self.running:
                    logger.error(action, logger.LogResult.fail, "error", str(e))
                    raise e
                continue

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
            t.join(timeout=THREAD_JOIN_TIMEOUT_SEC)

        self._close_server_socket()

