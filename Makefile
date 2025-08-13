# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make setup-cassandra			- Start Cassandra cluster using Docker Compose"
	@echo "  make teardown-cassandra        - Stop Cassandra cluster and remove containers"
	@echo "  make logs      				- View logs of Cassandra containers"
	@echo "  make cqlsh     				- Connect to cassandra1 using cqlsh"

# Start the cluster
.PHONY: setup-cassandra
setup-cassandra:
	docker-compose -f build/docker-compose.yml up -d

# Stop the cluster
.PHONY: teardown-cassandra
teardown-cassandra:
	docker-compose -f build/docker-compose.yml down

# View logs
.PHONY: logs
logs:
	docker-compose -f build/docker-compose.yml logs

# Connect to cassandra1
.PHONY: cqlsh
cqlsh:
	docker exec -it cassandra1 cqlsh 127.0.0.1 9042