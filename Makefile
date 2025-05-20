# Variáveis de configuração
REGION=
ACCOUNT_ID=
REPOSITORY_NAME=

lint:
	black . | isort . 
# Autenticação no ECR
login:
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com

# Build da imagem Docker
build:
	docker build --no-cache -t $(REPOSITORY_NAME) -f docker/Dockerfile .

# Tag da imagem
tag:
	docker tag $(REPOSITORY_NAME):latest $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPOSITORY_NAME):latest

# Push da imagem para o ECR
push:
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/$(REPOSITORY_NAME):latest

# Comando para executar todas as etapas
deploy: login build tag push

docker:
	docker run \
		-e DB_NAME="$(DB_NAME)" \
		-e DB_USER="$(DB_USER)" \
		-e DB_PASSWORD="$(DB_PASSWORD)" \
		-e DB_HOST="$(DB_HOST)" \
		-e AWS_BUCKET="$(AWS_BUCKET)" \
		-e AWS_REGION="$(AWS_REGION)" \
		-e AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
		-e HTML_QUEUE_NAME="$(HTML_QUEUE_NAME)" \
		-e QUEUE_NAME="$(QUEUE_NAME)" \
		$(REPOSITORY_NAME)