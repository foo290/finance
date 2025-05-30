import papermill as pm
import os
from pathlib import Path
import shutil


ROOT_DIR = Path(__file__).parent

transform_notebook_path = ROOT_DIR / 'finance' / 'pipelines' / 'transform.ipynb'
migrate_notebook_path = ROOT_DIR / 'finance' / 'pipelines' / 'migrate_data.ipynb'
# rename_notebook_path = ROOT_DIR / 'finance' / 'pipelines' / 'rename_file.ipynb'

data_file_path = '/Users/nitinsharma/playground/finance/data/'
transformed_file_path = '/Users/nitinsharma/playground/finance/transformed/'

os.makedirs(data_file_path, exist_ok=True)
os.makedirs(transformed_file_path, exist_ok=True)

for file in os.listdir(data_file_path):
    if file.endswith('.xls'):
        file_path = os.path.join(data_file_path, file)
        pm.execute_notebook(
            transform_notebook_path,
            transform_notebook_path,
            parameters={
                'source_file_path': file_path,
                'destination_file_path': '/Users/nitinsharma/playground/finance/transformed/'
            }
        )

processed_file_path = '/Users/nitinsharma/playground/finance/data/processed/'
os.makedirs(processed_file_path, exist_ok=True)
for file in os.listdir(data_file_path):
    if file.endswith('.xls'):
        file_path = os.path.join(data_file_path, file)
        shutil.move(file_path, processed_file_path)


print("Starting Migration")

for file in os.listdir(transformed_file_path):
    if file.endswith('.csv'):
        file_path = os.path.join(transformed_file_path, file)
        pm.execute_notebook(
            migrate_notebook_path,
            migrate_notebook_path,
            parameters={
                'file_path': file_path,
            }
        )


processed_transformed_file_path = '/Users/nitinsharma/playground/finance/transformed/processed/'
os.makedirs(processed_transformed_file_path, exist_ok=True)
for file in os.listdir(transformed_file_path):
    if file.endswith('.csv'):
        file_path = os.path.join(transformed_file_path, file)
        shutil.move(file_path, processed_transformed_file_path)

print("Migration completed")
