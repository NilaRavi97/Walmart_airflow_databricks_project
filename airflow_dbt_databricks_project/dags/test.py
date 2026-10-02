from databricks.sdk import WorkspaceClient
import time

ws = WorkspaceClient()

job_trigger = ws.jobs.run_now(job_id=1109396595478553)

while True:
    job_status = ws.jobs.get_run(run_id=job_trigger.run_id)

    
    if job_status.state.life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        if job_status.state.result_state == "SUCCESS":
            print("Job completed successfully.")
            break
        else:
            raise Exception(f"Job failed with state: {job_status.state.result_state}")
        
    time.sleep(10)  # Wait for 10 seconds before checking the status again