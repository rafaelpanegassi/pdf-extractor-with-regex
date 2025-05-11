import os

import boto3


class AWSSQSManager:
    def __init__(
        self,
        access_key: str = None,
        secret_key: str = None,
        region_name: str = "us-west-1",
    ):

        if (
            not self.check_environment_variables()
            and access_key is None
            and secret_key is None
        ):
            raise ValueError("As credenciais da AWS não foram fornecidas.")

        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = region_name or os.getenv("AWS_REGION")

        if not self.access_key or not self.secret_key:
            raise ValueError("As credenciais da AWS não foram fornecidas.")

        self.sqs = boto3.client(
            "sqs",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
        )

    def get_queue_url(self, queue_name):
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]
        except Exception as e:
            print(f"Erro ao obter URL da fila: {e}")
            return None

    def receive_messages_from_queue(
        self,
        queue_name: str,
        max_number_of_messages: int = 10,
        visibility_timeout: int = 30,
    ):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.get_queue_url(queue_name),
                MaxNumberOfMessages=max_number_of_messages,
                VisibilityTimeout=visibility_timeout,
                WaitTimeSeconds=0,
            )
            messages = response.get("Messages", [])
            return messages
        except Exception as e:
            print(f"Erro ao receber mensagens da fila: {e}")
            return []

    def check_message_in_queue(self, queue_name: str):
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.get_queue_url(queue_name),
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            approximate_number_of_messages = response.get("Attributes", {}).get(
                "ApproximateNumberOfMessages", "N/A"
            )
            print(
                f"Número aproximado de mensagens na fila: {approximate_number_of_messages}"
            )
            if int(approximate_number_of_messages) > 0:
                return True
            return False
        except Exception as e:
            print(f"Erro ao verificar mensagens na fila: {e}")

    def delete_message_from_queue(self, queue_name: str, receipt_handle: str):
        try:
            self.sqs.delete_message(
                QueueUrl=self.get_queue_url(queue_name), ReceiptHandle=receipt_handle
            )
            print("Mensagem deletada com sucesso.")
        except Exception as e:
            print(f"Erro ao deletar mensagem da fila: {e}")

    @staticmethod
    def check_environment_variables():
        if (
            not os.getenv("AWS_ACCESS_KEY_ID")
            or not os.getenv("AWS_SECRET_ACCESS_KEY")
            or not os.getenv("AWS_REGION")
        ):
            print(
                "As variáveis de ambiente AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY não estão configuradas."
            )
            return False
        else:
            print("Variáveis de ambiente configuradas corretamente.")
            return True