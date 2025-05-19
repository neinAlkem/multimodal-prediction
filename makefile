build:
	docker compose build --no-cache
down: 
	docker compose down --volumes --remove-orphans
run:
	make down && docker compose up --scale spark-worker=3
stop:
	docker compose stop
submit-cluster:
	docker exec da-spark-yarn-master spark-submit --master yarn --deploy-mode cluster ./apps/$(app)