import os
from airflow.exceptions import AirflowFailException

def check_logs_for_errors(context):
    log_base_path = os.getenv("AIRFLOW__CORE__BASE_LOG_FOLDER", "/opt/airflow/logs")
    task_instance = context['task_instance']

    # Construct the log file path
    log_file_path = os.path.join(
        log_base_path,
        "scheduler",
        task_instance.execution_date.strftime("%Y-%m-%d"),
        f"{task_instance.dag_id}.py.log"
    )
    task_instance.log.info(f"Checking logs for errors in: {log_file_path}")

    try:
        # Check if the log file exists
        if not os.path.isfile(log_file_path):
            raise FileNotFoundError(f"Log file not found: {log_file_path}")

        # Open and read the log file
        with open(log_file_path, "r") as log_file:
            log_lines = log_file.readlines()

        # Check for actual errors (not just "ERROR" mentions)
        error_lines = [line for line in log_lines if "ERROR" in line and "Exception" in line]
        
        if error_lines:
            task_instance.log.error(f"Found error lines: {error_lines}")
            raise AirflowFailException(f"Errors found in log file: {log_file_path}")

    except AirflowFailException as e:
        # Log and propagate specific Airflow errors
        task_instance.log.error(f"Airflow error detected: {e}")
        raise e
 
    except Exception as e:
        # Catch all other exceptions and re-raise them as Airflow exceptions
        task_instance.log.error(f"Unexpected error: {e}")
        raise AirflowFailException(f"Unexpected error: {e}")


def task_failure_callback(context):
    """
    Callback triggered on task failure.
    """
    task_instance = context['task_instance']
    task_instance.log.error(
        f"Task {task_instance.task_id} in DAG {task_instance.dag_id} failed "
        f"on {task_instance.execution_date}. See logs for details."
    )


def task_success_callback(context):
    """
    Callback triggered on task success.
    """
    task_instance = context['task_instance']
    try:
        task_instance.log.info("Checking task logs for errors post-success.")
        check_logs_for_errors(context)
    except AirflowFailException as e:
        # Log the specific failure reason
        task_instance.log.error(f"Task failed due to: {e}")
        raise
    except Exception as e:
        # Handle unexpected errors gracefully
        task_instance.log.error(f"Unhandled error in callback: {e}")
        raise
