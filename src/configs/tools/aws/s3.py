import os

import boto3


class AWSS3:
    _instance = None

    def __new__(cls, access_key=None, secret_key=None, region_name=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            if (
                not cls._instance.check_environment_variables()
                and access_key is None
                and secret_key is None
            ):
                raise ValueError("As credenciais da AWS não foram fornecidas.")

            cls._instance.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
            cls._instance.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
            cls._instance.region_name = region_name or os.getenv("AWS_REGION")

            cls._instance.s3 = boto3.client(
                "s3",
                aws_access_key_id=cls._instance.access_key,
                aws_secret_access_key=cls._instance.secret_key,
                region_name=cls._instance.region_name,
            )
        return cls._instance

    def download_file_from_s3(self, bucket_name, key, local_file_path):
        try:
            with open(local_file_path, "wb") as f:
                self.s3.download_fileobj(bucket_name, key, f)
        except Exception as e:
            raise (f"Erro ao baixar arquivo: {e}")

    def upload_file_to_s3(self, bucket_name, key, local_file_path):
        try:
            self.s3.upload_file(local_file_path, bucket_name, key)
            return True
        except Exception as e:
            print(e)
            return False

    def delete_file_from_s3(self, bucket_name, key):
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=key)
        except Exception as e:
            print(f"Erro ao deletar arquivo do S3: {e}")

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