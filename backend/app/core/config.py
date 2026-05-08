from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # SQL Server
    sqlserver_host: str
    sqlserver_driver: str = "ODBC Driver 18 for SQL Server"
    sqlserver_linked_host: str

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "cargas_neotel"
    postgres_user: str
    postgres_password: str

    # FTP principal
    ftp_host: str = ""
    ftp_port: int = 22
    ftp_user: str = ""
    ftp_password: str = ""
    ftp_base: str = "/archivos"

    # FTP servidor 17
    neotel17_ftp_host: str
    neotel17_ftp_port: int
    neotel17_ftp_user: str
    neotel17_ftp_password: str

    # JWT
    auth_secret_key: str
    auth_token_minutes: int = 480

    # LDAP
    ldap_host: str
    ldap_port: int = 389
    ad_domain: str
    ad_base_dn: str

    # App
    app_env: str = "development"
    secret_key: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()