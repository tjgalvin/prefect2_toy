import copy
import os
import subprocess 
import asyncio

import numpy as np 
from prefect_dask.task_runners import DaskTaskRunner
from prefect import flow, task
from dask_jobqueue import SLURMCluster

DOCKER_CONTAINER='nlknguyen/alpine-mpich'

        
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

    return result.stdout
    

# --------------------------------------
# Flows
def make_subflow(name):
    @flow(
    name=f"Silly-name-{name}",
    task_runner=DaskTaskRunner(
            cluster_class="ScaleSLURMCluster.ScaleSLURMCluster",
            cluster_kwargs=dict(
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
        )
    )
    async def srun_flow(some_int=1):
        """A flow that will be wrapped up later to represent an
        MPI application to execute. In this example it is a simple
        hello world invoked through a subprocess. 

        This function is turned into a flow from the main(), using 
        the create_run_subflow function. 
        """
        print("Running run_flow now")
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
    
    vals = [make_subflow(0)(0), make_subflow(1)(1)]
    collection = await asyncio.gather(
        *vals
    )

    print("Printing the collection")
    print("\n".join([
        c.result() for c in collection
    ]))

    # Database locks can still happen -- poor little sqlite
    # collection = await asyncio.gather( 
    #     *[make_subflow(f"Some other name {i}")(i) for i in range(4)]
    # )

    # print("Printing the collection")
    # print("\n".join([
    #     c.result() for c in collection
    # ]))
    


if __name__ == "__main__":
    asyncio.run(main())

