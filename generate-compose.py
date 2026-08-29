import sys

num_clients = 1  # Default
if len(sys.argv) > 1:
    try:
        num_clients = int(sys.argv[1])
        if num_clients < 1:
            raise ValueError("Number of clients must be greater than 0")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

quorum = min(2, num_clients)

compose_content = f"""services:
  server:
    build:
      context: ./services/server
      dockerfile: Dockerfile
    container_name: server
    ports:
      - "5678:5678"
    environment:
      - PYTHONUNBUFFERED=1
      - SERVER_HOST=server
      - SERVER_PORT=5678
      - AGENCY_QUORUM_MIN={quorum}

"""

for i in range(num_clients):
    compose_content += f"""  client_{i}:
    build:
      context: ./services/client
      dockerfile: Dockerfile
    container_name: client_{i}
    depends_on:
      - server
    environment:
      - AGENCY_ID={i}
      - SERVER_HOST=server
      - SERVER_PORT=5678
      - INPUT_FILE=/input/input-{i}.csv
      - OUTPUT_FILE=/output/output-{i}.csv
      - BATCH_SIZE=3
    volumes:
      - ./input:/input
      - ./output:/output
"""

with open("docker-compose.yaml", "w") as f:
    f.write(compose_content)