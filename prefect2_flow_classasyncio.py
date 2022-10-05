import os
import subprocess 
import asyncio
import random
from time import sleep

import numpy as np 
from prefect_dask.task_runners import DaskTaskRunner
from prefect import flow, task
from dask_jobqueue import SLURMCluster

DOCKER_CONTAINER='nlknguyen/alpine-mpich'

def print_result_collection(collection):
    print(f"Printing result collection of {len(collection)=}")
    print(
        "\n".join(
            [c.result().strip() for c in collection]
        )
    )


        
# --------------------------------------
# Tasks

@task
def make_random_array(n):
    print(f"Number of floats: {np.prod(n)}")
    return np.random.random(n).astype('f4')


@task
async def srun_run(some_int=99):

    print('Running srun business now')
    print(f'Received {some_int=}')

    random.seed(some_int)
    rand_sleep = random.randint(10,100)
    print(f"Sleeping for {rand_sleep}")
    sleep(rand_sleep)

    srun_str = (
        f'srun --mpi=pmi2 -N 1 -n 1 '
        f'singularity exec -B $(pwd) {DOCKER_CONTAINER.split("/")[1]}.sif '
        f'./mpi_hello_world'
    )

    # srun_str = (
    #     f"sleep 2 && echo finished sleeping for {some_int}"
    # )

    result = subprocess.run(
        srun_str,
        shell=True,
        capture_output=True,
        text=True
    )
 
    print(result.stderr)
    print(result.stdout)

    print(result.args)

    return f"I am returning {some_int} and I was asleep for {rand_sleep} seconds"
    

# --------------------------------------
# Flows
def make_subflow(name, count):
    @flow(
    name=f"Silly-name-{name}",
    task_runner=DaskTaskRunner(
            # cluster_class="ScaleSLURMCluster.ScaleSLURMCluster",
            cluster_class="dask_jobqueue.SLURMCluster",
            cluster_kwargs=dict(
                n_workers=1,
                scheduler_options=dict(dashboard_address=f':{32120+count}'),                cores=1,
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
        )
    )
    async def srun_flow(some_int=1):
        """A flow that will be wrapped up later to represent an
        MPI application to execute. In this example it is a simple
        hello world invoked through a subprocess. 

        This function is turned into a flow from the main(), using 
        the create_run_subflow function. 
        """
        srun_result = srun_run.submit(some_int)

        return await srun_result

    return srun_flow

@flow(
    task_runner=DaskTaskRunner(
        cluster_kwargs={
            "n_workers": 1, 
            "resources": {"process": 2, "threads_per_worker": 10}, 
            "local_directory": os.getcwd()
        },
    )
)
async def main():
    """The main flow with a set of dummy operaitons
    """
    # Define the work
    shape = (600,900)
    a = make_random_array.submit(shape)
    
    print("Running a small collection of subflows...")

    # Database locks can still happen -- poor little sqlite
    collection = await asyncio.gather( 
        *[make_subflow(f"Some other name {i}", i)(i) for i in range(100)]
    )
    print_result_collection(collection)
    


if __name__ == "__main__":
    
    asyncio.run(main())

