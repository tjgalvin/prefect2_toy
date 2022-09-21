
import os
import subprocess 

import numpy as np 
from prefect_dask.task_runners import DaskTaskRunner
from prefect import flow, task
from dask_jobqueue import SLURMCluster
from anyio import create_task_group, run, sleep

DOCKER_CONTAINER='nlknguyen/alpine-mpich'

# --------------------------------------
# Tasks

@task
def make_random_array(n):
    print(f"Number of floats: {np.prod(n)}")
    return np.random.random(n).astype('f4')

@task 
def print_array(n):
    print(f"Array: {n.nbytes=} {n.shape=}")

@task
def find_mean(a):
    print("Finding the mean of a")
    return a.mean()

@task
def add_arrays(a,b):
    print("Adding the arrays a and b")
    return a + b

@task
def print_mean(m):
    print(f"The mean is {m}")
    

@task
def srun_run(some_int=99):

    print('Running srun business now')
    print(f'Received {some_int=}')

    srun_str = (
        f'srun --mpi=pmi2 -N 1 -n 1 '
        f'singularity exec -B $(pwd) docker://{DOCKER_CONTAINER} '
        f'./mpi_hello_world'
    )

    result = subprocess.run(
        srun_str,
        shell=True,
        capture_output=True,
        text=True
    )
 
    print(result.stderr)
    print(result.stdout)

    print(result.args)

    return f'Hi there, count {some_int=}'


# --------------------------------------
# Flows

def srun_flow(some_int=1):
    """A flow that will be wrapped up later to represent an
    MPI application to execute. In this example it is a simple
    hello world invoked through a subprocess. 

    This function is turned into a flow from the main(), using 
    the create_run_subflow function. 
    """
    srun_result = srun_run.submit(some_int)

    return srun_result


@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "n_workers": 1, 
            "resources": {"process": 2, "threads_per_worker": 10}, 
            "local_directory": os.getcwd()
        },
    )
)
def main():
    """The main flow with a set of dummy operaitons
    """
    # Define the work
    shape = (600,900)
    a = make_random_array.submit(shape)
    print_array.submit(a)

    b = make_random_array.submit(shape)
    print_array.submit(b)
    
    c = add_arrays.submit(a, b)
    d = find_mean.submit(c)
    
    print_mean.submit(d)

    # Here, the parallelism is not as nice as one would hope, as when a subflow
    # is entered there is now a _blocking_ operation haulting the creating further. 
    print("Running a small collection of subflows...")
    collection = [create_run_subflow(srun_flow, some_int=si) for si in range(3)]

    print(f"{len(collection)}")
    print('\n'.join(collection))


    # The code extract below has not really worked. But, including it here
    # for reference. 

    # r = await create_run_multi_subflow(
    #     srun_flow,
    #     list(range(2))
    # )
    # results = await asyncio.gather(*r)
        
    
# --------------------------------------
# Helper code

def make_slurm_cluster():
    """Creates the SLURM request, which includes starting up a dask-worked
    on the slurm allocation. 

    Note that it is important to ensure that the same conda environment is
    being used. 

    The tweaking of the SINGULARITY variables are to undo some of the 
    setup options made by the module load singularity
    """
    cluster = SLURMCluster(
            cores=1,
            processes=1,
            name='Tester',
            memory='60GB',
            queue='workq',
            project='askap',
            walltime='00:15:00',
            job_extra_directives=[
                '-M galaxy',
                '--reservation askapdev',
                f'-N 1',
            ],
            interface='ipogif0',
            job_script_prologue=[
            'export OMP_NUM_THREADS=1',
            'source /home/$(whoami)/.bashrc',
            'conda activate prefect3.10',
            'module load singularity',
            'unset SINGULARITY_BINDPATH',
            'unset SINGULARITYENV_LD_LIBRARY_PATH'
            ],
    )

    print("Scaling up...")
    cluster.scale(
            n=1,
    )
    print("...scaled up!")

    return cluster

def create_run_subflow(func, *args, **kwargs):
    """Will wrap around a function to create a flow. Importantly,
    this function will also spin up a new SLURM request and start
    a dask worker to carry out the computation. 

    Some marshalling of the prefect structure to returned result (for 
    convenience).
    """
    jobq_cluster = make_slurm_cluster()
    jobq_dask = DaskTaskRunner(jobq_cluster.scheduler_address)
    
    flow_func = flow(
        func,
        task_runner=jobq_dask
    )

    print(f"{type(flow_func)=} {args=} {kwargs=}")

    flow_results = flow_func(*args, **kwargs).result()

    print(f"Caught flow_results: {flow_results=}")

    jobq_cluster.close()
    
    return flow_results

def create_run_multi_subflow(func, iterable, *args, **kwargs):
    """Will wrap around a function to create a flow. Importantly,
    this function will also spin up a new SLURM request and start
    a dask worker to carry out the computation. 

    Some marshalling of the prefect structure to returned result (for 
    convenience).

    This has not been successful so far...
    """
    # async def async_func(*args, **kwargs):
    #     return func(*args, **kwargs)
    
    items = []
    for i in iterable:
        jobq_cluster = make_slurm_cluster()
        jobq_dask = DaskTaskRunner(jobq_cluster.scheduler_address)
        
        flow_func = flow(
            func,
            task_runner=jobq_dask
        )
        items.append(flow_func())

    return items


if __name__ == "__main__":
    main()

