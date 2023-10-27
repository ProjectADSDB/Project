import os
import psutil

def monitor_performance():
    """
    Function to print basic system performance metrics.
    """
    print("\n--- System Performance Metrics ---")
    print(f"CPU Usage: {psutil.cpu_percent()}%")
    print(f"Memory Usage: {psutil.virtual_memory().percent}%")
    print("----------------------------------\n")


def execute_script(script_name):
    """
    Execute a given Python script and handle potential errors.
    """
    try:
        os.system(f"python {script_name}")
        print(f"Execution of {script_name} completed successfully!")
    except Exception as e:
        print(f"Error executing {script_name}. Error: {e}")
    monitor_performance()


def main():
    """
    Main function to orchestrate the execution of Python scripts.
    """
    for script in ["file_separation.py", "formatted_zone.py", "check_duplicates.py", data_profiling.py", "trusted_zone.py", "exploitation_zone.py", "data_analysis.py"]:
        execute_script(script)

if __name__ == "__main__":
    main()
