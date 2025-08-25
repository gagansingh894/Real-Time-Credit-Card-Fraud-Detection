# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make setup						- Start Cassandra, Spark cluster using Docker Compose"
	@echo "  make teardown        			- Stop Cassandra cluster and remove containers"
	@echo "  make logs      				- View logs of Cassandra containers"
	@echo "  make cqlsh     				- Connect to cassandra1 using cqlsh"

# Start the cluster
.PHONY: setup
setup:
	docker build -t gagansingh894/airflow:latest -f build/Dockerfile.airflow .
	docker build -t gagansingh894/spark:latest -f build/Dockerfile.spark .
	docker-compose -f build/docker-compose.yml up -d

# Stop the cluster
.PHONY: teardown
teardown:
	docker-compose -f build/docker-compose.yml down

# View logs
.PHONY: logs
logs:
	docker-compose -f build/docker-compose.yml logs

# Connect to cassandra1
.PHONY: cqlsh
cqlsh:
	docker exec -it cassandra1 cqlsh 127.0.0.1 9042