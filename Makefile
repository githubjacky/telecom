.PHONY: mysql build clean preprocess mlflow jupynium jupyter


mysql:
	./script/mysql.sh

# docker
build:
	docker compose build


clean:
	docker rmi --force 0jacky/telecom:latest


preprocess:
	docker compose run --rm preprocess


mlflow:
	docker compose run --rm --service-ports mlflow-ui


jupynium:
	poetry run jupynium --nvim_listen_addr localhost:18898 --notebook_URL localhost:8891/nbclassic


jupyter:
	docker compose run --rm --service-ports jupyter-notebook
