.PHONY: mysql preprocess mlflow jupyter


mysql:
	./script/mysql.sh


# docker
preprocess:
	docker compose run --rm preprocess


mlflow:
	docker compose run --rm --service-ports mlflow-ui


jupyter:
	docker compose run --rm --service-ports jupyter-lab
