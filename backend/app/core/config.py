from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # SQL Server
    sqlserver_host: str
    sqlserver_user: str
    sqlserver_password: str
    sqlserver_driver: str = "ODBC Driver 17 for SQL Server"

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "cargas_neotel"
    postgres_user: str
    postgres_password: str

    # FTP
    ftp_host: str = ""
    ftp_port: int = 21
    ftp_user: str = ""
    ftp_password: str = ""
    ftp_path: str = "/"

    # App
    app_env: str = "development"
    secret_key: str = "SD6bwCItIGEXsyGN8sbKkACb4DvHln3NAyDFAQrPlkI"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
