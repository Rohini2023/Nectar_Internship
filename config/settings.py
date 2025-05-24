from .env_loader import load_environment
import os

load_environment()  # Load .env.development or .env.production before reading env vars

class Config:
    CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
    CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
    CASSANDRA_LOCAL_DC=os.getenv("CASSANDRA_LOCAL_DC")
    CASSANDRA_PROTOCOL_VERSION=int(os.getenv("CASSANDRA_PROTOCOL_VERSION"))
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    TOKEN_API_URL = os.getenv("TOKEN_API_URL")
    API_USERNAME = os.getenv("API_USERNAME")
    API_PASSWORD = os.getenv("API_PASSWORD")
    TIMEZONE = os.getenv("TIMEZONE")

settings = Config()
