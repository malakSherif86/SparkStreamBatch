import os
import subprocess
from datetime import datetime

# Define the base data directory and HDFS destination directory
base_data_dir = '/data'
group_dirs = [f'{base_data_dir}/group{i}' for i in range(1, 7)]
state_file = '/data/state_file.txt'

# Function to get the current date and hour
def get_current_datetime():
    now = datetime.now()
    return now.strftime("%Y-%m-%d"), now.strftime("%H")

def get_destination_dirs():
    current_date = get_current_datetime()[0]
    return {
        'sales_transactions': f'/finalData/spark_project/sales_transactions2/date({current_date})',
        'branches': f'/finalData/spark_project/branches2/date({current_date})',
        'sales_agents': f'/finalData/spark_project/sales_agents2/date({current_date})'
    }

def get_current_directory_index():
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            content = f.read().strip()
            if content.isdigit():
                return int(content)
    return 0

def update_current_directory_index(current_index):
    next_index = (current_index + 1) % len(group_dirs)
    with open(state_file, 'w') as f:
        f.write(str(next_index))

def get_destination_directory(file_name, destination_dirs):
    for prefix, destination in destination_dirs.items():
        if file_name.startswith(prefix):
            return destination
    return None

def ensure_hdfs_directory_exists(hdfs_dir):
    try:
        subprocess.run(["hdfs", "dfs", "-test", "-d", hdfs_dir], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        try:
            subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
            print(f"Created HDFS directory: {hdfs_dir}")
        except subprocess.CalledProcessError as e:
            print(f"Error creating HDFS directory {hdfs_dir}: {e}")
            raise

def move_files_to_hdfs():
    try:
        current_index = get_current_directory_index()
        source_dir = group_dirs[current_index]

        # Get list of files in the source directory
        files = os.listdir(source_dir)

        # Check if there are files to move
        if not files:
            print(f"No files to move in {source_dir}.")
            return

        destination_dirs = get_destination_dirs()
        current_hour = get_current_datetime()[1]

        # Move each file to the appropriate HDFS destination directory
        for file_name in files:
            local_file_path = os.path.join(source_dir, file_name)
            destination_dir = get_destination_directory(file_name, destination_dirs)
            
            if destination_dir is None:
                print(f"No matching destination for {file_name}, skipping.")
                continue

            # Concatenate the current hour to the file name
            base_name, ext = os.path.splitext(file_name)
            new_file_name = f"{base_name}_{current_hour}{ext}"
            hdfs_file_path = os.path.join(destination_dir, new_file_name)
            
            print(f"Moving {file_name} to {hdfs_file_path}")
            if os.path.isfile(local_file_path):
                command = ["hdfs", "dfs", "-put", local_file_path, hdfs_file_path]
                
                # Ensure destination directory exists in HDFS
                ensure_hdfs_directory_exists(os.path.dirname(hdfs_file_path))
                
                try:
                    subprocess.run(command, check=True)
                    print(f"Successfully moved {file_name} to {hdfs_file_path}")
                except subprocess.CalledProcessError as e:
                    print(f"Error moving {file_name} to HDFS: {e}")

        print(f"Files moved to HDFS from {source_dir} at: {datetime.now()}")

        # Update state file to the next directory index
        update_current_directory_index(current_index)

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    move_files_to_hdfs()
