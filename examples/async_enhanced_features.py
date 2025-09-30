"""
Enhanced async workflow example showcasing new features:
- Error handling and debugging
- Timeout support
- Enhanced concurrency primitives
- Async context management
- Cleanup tasks
"""

import os
import time
from datetime import timedelta

from durabletask.aio import AsyncWorkflowContext, SandboxMode
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def slow_activity(_, duration: float) -> str:
    """Simulate a slow activity that takes the specified duration."""
    time.sleep(duration)
    return f'Completed after {duration} seconds'


def unreliable_activity(_, fail_probability: float) -> str:
    """Activity that may fail based on probability."""
    import random

    if random.random() < fail_probability:
        raise Exception(f'Activity failed (probability: {fail_probability})')
    return 'Activity succeeded'


async def enhanced_workflow_example(ctx: AsyncWorkflowContext, config: dict) -> dict:
    """
    Demonstrates enhanced async workflow features.

    Args:
        config: Dictionary with workflow configuration:
            - timeout_seconds: Timeout for activities
            - parallel_tasks: Number of parallel tasks to run
            - use_cleanup: Whether to register cleanup tasks
            - enable_debug: Whether to enable debug logging
    """

    # Use async context manager for automatic cleanup
    async with ctx:
        try:
            # Register cleanup tasks if requested
            if config.get('use_cleanup', False):
                cleanup_data = []

                def cleanup_task():
                    cleanup_data.append('Workflow cleanup executed')
                    print(f'[{ctx.instance_id}] Cleanup task executed')

                ctx.add_cleanup(cleanup_task)

            # Get debug info if debug mode is enabled
            if config.get('enable_debug', False):
                debug_info = ctx.get_debug_info()
                print(f'[{ctx.instance_id}] Debug info: {debug_info}')

            # Example 1: Activity with timeout
            timeout_seconds = config.get('timeout_seconds', 5.0)
            print(f'[{ctx.instance_id}] Running activity with {timeout_seconds}s timeout')

            try:
                # This will timeout if the activity takes too long
                result = await ctx.with_timeout(
                    ctx.activity(slow_activity, input=2.0),  # 2 second activity
                    timeout_seconds,
                )
                print(f'[{ctx.instance_id}] Activity completed: {result}')
            except TimeoutError as e:
                print(f'[{ctx.instance_id}] Activity timed out: {e}')
                result = 'Activity timed out'

            # Example 2: Enhanced when_any with result index
            print(f'[{ctx.instance_id}] Running when_any_with_result example')

            activities = [
                ctx.activity(slow_activity, input=1.0),
                ctx.activity(slow_activity, input=2.0),
                ctx.activity(slow_activity, input=3.0),
            ]

            # Get both the index and result of the first completed task
            completed_index, completed_result = await ctx.when_any_with_result(activities)
            print(f'[{ctx.instance_id}] Task {completed_index} completed first: {completed_result}')

            # Example 3: Parallel execution with error handling
            parallel_tasks = config.get('parallel_tasks', 3)
            print(
                f'[{ctx.instance_id}] Running {parallel_tasks} parallel tasks with error handling'
            )

            # Create tasks with different failure probabilities
            risky_tasks = [
                ctx.activity(unreliable_activity, input=0.2),  # 20% failure rate
                ctx.activity(unreliable_activity, input=0.3),  # 30% failure rate
                ctx.activity(unreliable_activity, input=0.1),  # 10% failure rate
            ][:parallel_tasks]

            # Use gather with return_exceptions to handle failures gracefully
            results = await ctx.gather(*risky_tasks, return_exceptions=True)

            successes = []
            failures = []

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failures.append(f'Task {i}: {result}')
                else:
                    successes.append(f'Task {i}: {result}')

            print(f'[{ctx.instance_id}] Successes: {len(successes)}, Failures: {len(failures)}')

            # Example 4: Deterministic random operations
            print(f'[{ctx.instance_id}] Demonstrating deterministic randomness')

            # These will be the same on replay
            random_uuid = ctx.uuid4()
            random_number = ctx.random().randint(1, 100)

            print(f'[{ctx.instance_id}] Generated UUID: {random_uuid}')
            print(f'[{ctx.instance_id}] Random number: {random_number}')

            # Example 5: Sleep with different duration types
            print(f'[{ctx.instance_id}] Testing different sleep duration types')

            # Sleep using timedelta
            await ctx.sleep(timedelta(milliseconds=500))

            # Sleep using float seconds
            await ctx.sleep(0.5)

            # Example 6: External event with timeout
            print(f'[{ctx.instance_id}] Waiting for external event with timeout')

            try:
                event_data = await ctx.with_timeout(
                    ctx.wait_for_external_event('user_input'),
                    2.0,  # 2 second timeout
                )
                print(f'[{ctx.instance_id}] Received event: {event_data}')
            except TimeoutError:
                print(f'[{ctx.instance_id}] No external event received within timeout')
                event_data = None

            # Final result
            final_result = {
                'instance_id': ctx.instance_id,
                'workflow_name': ctx.workflow_name,
                'first_activity_result': result,
                'first_completed_task': completed_index,
                'parallel_successes': len(successes),
                'parallel_failures': len(failures),
                'random_uuid': str(random_uuid),
                'random_number': random_number,
                'external_event_received': event_data is not None,
                'timestamp': ctx.now().isoformat(),
            }

            # Get final debug info
            if config.get('enable_debug', False):
                final_debug = ctx.get_debug_info()
                final_result['debug_info'] = final_debug
                print(f'[{ctx.instance_id}] Final debug info: {final_debug}')

            return final_result

        except Exception as e:
            print(f'[{ctx.instance_id}] Workflow failed: {e}')
            # Return error information
            return {
                'instance_id': ctx.instance_id,
                'error': str(e),
                'error_type': type(e).__name__,
                'timestamp': ctx.now().isoformat(),
            }


def main():
    # Enable debug mode for demonstration
    os.environ['DAPR_WF_DEBUG'] = 'true'

    # Point to your sidecar, or set DURABLETASK_GRPC_ENDPOINT in env
    os.environ.setdefault('DURABLETASK_GRPC_ENDPOINT', 'localhost:4001')

    with TaskHubGrpcWorker() as worker:
        # Register activities
        worker.add_activity(slow_activity)
        worker.add_activity(unreliable_activity)

        # Register the enhanced workflow with strict sandboxing
        worker.add_async_orchestrator(
            enhanced_workflow_example,
            name='enhanced_workflow',
            sandbox_mode=SandboxMode.STRICT,  # Use strict mode for demonstration
        )

        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()

        # Test configuration
        test_config = {
            'timeout_seconds': 10.0,
            'parallel_tasks': 3,
            'use_cleanup': True,
            'enable_debug': True,
        }

        instance_id = client.schedule_new_orchestration('enhanced_workflow', input=test_config)
        print(f'Started enhanced workflow instance: {instance_id}')

        # Optionally send an external event after a delay
        import threading

        def send_event():
            time.sleep(1)  # Wait 1 second
            try:
                client.raise_event(instance_id, 'user_input', 'Hello from external event!')
                print(f'Sent external event to {instance_id}')
            except Exception as e:
                print(f'Failed to send external event: {e}')

        # Start event sender in background
        event_thread = threading.Thread(target=send_event)
        event_thread.start()

        # Wait for completion
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)
        if state:
            if state.failure_details:
                print('Workflow failed:')
                print(f'  Error: {state.failure_details.error_type}')
                print(f'  Message: {state.failure_details.message}')
                print(f'  Stack: {state.failure_details.stack_trace}')
            else:
                print(f'Workflow completed successfully!')
                print(f'Result: {state.serialized_output}')
        else:
            print('Workflow did not complete within timeout')

        # Wait for event thread to complete
        event_thread.join()

        now = time.time()
        # Wait for worker to be idle (no in-flight activities that could be running in the background)
        worker.wait_for_idle(timeout=8)
        print(f'Worker took {time.time() - now} seconds to finish all activities')


if __name__ == '__main__':
    main()
