from dotenv import load_dotenv
import os

def load_environment():
    env_name = os.getenv("ENV", "development")
    env_file = f".env.{env_name}"
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        raise FileNotFoundError(f"Environment file '{env_file}' not found.")


    print(f"ENV: {os.getenv('ENV')}")
    print(f"CASSANDRA_HOST: {os.getenv('CASSANDRA_HOST')}")