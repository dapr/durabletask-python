import os
import time
from datetime import timedelta

from durabletask.aio import AsyncWorkflowContext
from durabletask.client import TaskHubGrpcClient
from durabletask.task import NonRetryableError, RetryPolicy
from durabletask.worker import TaskHubGrpcWorker


def risky_activity(_, n: int) -> int:
    """Activity that fails non-retryably when given a negative number."""
    if n < 0:
        raise NonRetryableError('Negative input is not allowed')
    return n * 2


async def non_retryable_workflow(ctx: AsyncWorkflowContext, n: int) -> int:
    """Demonstrate that NonRetryableError avoids retries and fails the workflow immediately.

    - If input is negative, activity raises NonRetryableError; retry policy is ignored.
    - If input is non-negative, activity succeeds.
    """
    # Intentionally provide a retry policy to show that NonRetryableError short-circuits retries
    rp = RetryPolicy(first_retry_interval=timedelta(seconds=1), max_number_of_attempts=5)
    return await ctx.call_activity(risky_activity, input=n, retry_policy=rp)


def main():
    # Configure endpoint (override as needed)
    os.environ.setdefault('DURABLETASK_GRPC_ENDPOINT', 'localhost:4001')

    with TaskHubGrpcWorker() as worker:
        worker.add_activity(risky_activity)
        worker.add_orchestrator(non_retryable_workflow)
        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()

        print('\nCase 1: Non-retryable failure (n = -1)')
        inst1 = client.schedule_new_orchestration(non_retryable_workflow, input=-1)
        state1 = client.wait_for_orchestration_completion(inst1, timeout=60)
        if state1 and state1.failure_details:
            print('Status:', state1.runtime_status)
            print('Error type:', state1.failure_details.error_type)
            print('Message:', state1.failure_details.message)
        else:
            print('Unexpected success:', state1 and state1.serialized_output)

        print('\nCase 2: Success (n = 5)')
        inst2 = client.schedule_new_orchestration(non_retryable_workflow, input=5)
        state2 = client.wait_for_orchestration_completion(inst2, timeout=60)
        if state2:
            state2.raise_if_failed()
            print('Output:', state2.serialized_output)

        time.sleep(1)


if __name__ == '__main__':
    main()
