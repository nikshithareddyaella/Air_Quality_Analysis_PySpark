import os
import socket
import shutil
import time

PENDING_FOLDER = "ingestion/data/pending/prepared"
PROCESSED_FOLDER = "ingestion/data/processed"
HOST = "localhost"
PORT = 9999

def send_file_data(client_socket, file_path):
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if line:
                client_socket.sendall((line + "\n").encode("utf-8"))
                time.sleep(0.01)  # Small delay to simulate real streaming

def start_server():
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Important fix!
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)

    print(f"✅ TCP server listening on {HOST}:{PORT}...")

    while True:
        print("Waiting for a new client connection...")
        client_socket, address = server_socket.accept()
        print(f"✅ Client connected: {address}")

        for filename in sorted(os.listdir(PENDING_FOLDER)):
            file_path = os.path.join(PENDING_FOLDER, filename)
            if os.path.isfile(file_path):
                print(f"📄 Sending: {filename}")
                send_file_data(client_socket, file_path)
                shutil.move(file_path, os.path.join(PROCESSED_FOLDER, filename))
                print(f"✅ Finished: {filename}")
                time.sleep(10)  # Simulate 10-second delay between batches

        client_socket.close()
        print(f"🔴 Client disconnected: {address}")

if __name__ == "__main__":
    start_server()
