# import sys
# import os
# import logging

# # Step 1: Add project root to path (this must be done FIRST)
# project_root = r"C:\Users\ACER\Desktop\Internship Nectar Rohini\project15.5.2025\project14.5.2025"
# sys.path.append(project_root)

# # Step 2: Now import modules from app and config
# from app.cassandra_ops import connect_to_cassandra
# from config.settings import settings

# # Step 3: Set up logging
# logging.basicConfig(level=logging.INFO)

# # Step 4: Connect to Cassandra
# cluster, session = connect_to_cassandra()

# # Step 5: Read and execute CQL statements
# sql_file_path = os.path.join(project_root, 'DB', 'insert_queries.sql')
# with open(sql_file_path, 'r') as file:
#     sql_content = file.read()

# queries = sql_content.split(';')

# for query in queries:
#     query = query.strip()
#     if query:
#         try:
#             session.execute(query)
#             print(f"Executed: {query}")
#         except Exception as e:
#             print(f"Error executing query: {query}")
#             print(e)

# # Step 6: Shutdown cluster
# cluster.shutdown()
