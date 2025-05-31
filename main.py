import papermill as pm
import os
from pathlib import Path
import shutil


join_path = lambda *args: os.path.join(*args)

ROOT_DIR = Path(__file__).parent

transform_notebook_path = join_path(ROOT_DIR, 'finance', 'pipelines', 'transform.ipynb')
migrate_notebook_path = join_path(ROOT_DIR, 'finance', 'pipelines', 'migrate_data.ipynb')

source_file_path = join_path(ROOT_DIR, 'data')
destination_file_path = join_path(ROOT_DIR, 'transformed')

source_processed_file_path = join_path(ROOT_DIR, 'data', 'processed')
destination_processed_file_path = join_path(ROOT_DIR, 'transformed', 'processed')

os.makedirs(source_file_path, exist_ok=True)
os.makedirs(destination_file_path, exist_ok=True)

for file in os.listdir(source_file_path):
    if file.endswith('.xls'):
        file_path = os.path.join(source_file_path, file)
        pm.execute_notebook(
            transform_notebook_path,
            transform_notebook_path,
            parameters={
                'source_file_path': file_path,
                'destination_file_path': destination_file_path
            }
        )

os.makedirs(source_processed_file_path, exist_ok=True)
os.makedirs(destination_processed_file_path, exist_ok=True)
for file in os.listdir(source_file_path):
    if file.endswith('.xls'):
        file_path = os.path.join(source_file_path, file)
        shutil.move(file_path, source_processed_file_path)


print("Starting Migration")

pm.execute_notebook(
    migrate_notebook_path,
    migrate_notebook_path,
    parameters={
        'transformed_files_path': destination_file_path,
        "table_name": "transactions"
    }
)


os.makedirs(destination_processed_file_path, exist_ok=True)
for file in os.listdir(destination_file_path):
    if file.endswith('.csv'):
        file_path = os.path.join(destination_file_path, file)
        try:
            shutil.move(file_path, destination_processed_file_path)
        except Exception as e:
            print(f"Error moving file {file_path}: {e}")

print("Migration completed")
