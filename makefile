build:
	docker compose build --no-cache
down:
	docker compose down --volumes
run-scaled:
	echo "Starting Spark Cluster.."
	make down && docker compose up --scale spark-worker=$(WORKERS)
stop:
	docker compose stop
