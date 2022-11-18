import os
import subprocess 
import asyncio
import random
from time import sleep

import numpy as np 
from prefect_dask.task_runners import DaskTaskRunner
from prefect import get_run_logger
from prefect import flow, task
from dask_jobqueue import SLURMCluster

DOCKER_CONTAINER='nlknguyen/alpine-mpich'

def print_result_collection(collection):
    print(f"Printing result collection of {len(collection)=}")
    try:
        print(
            "\n\n\n".join(
            [c.result().strip() for c in collection]
            )
        )
    except:
       "\n\n\n".join( 
           [c.strip() for c in collection]
        )


        
# --------------------------------------
# Tasks

@task
async def sleeper(*args):
    subprocess.run(
        "sleep 5", shell=True
    )
    return args


@task
def make_random_array(n):
    logger = get_run_logger()
    logger.info('Tim was here')
    logger.info(f"Number of floats: {np.prod(n)}")
    return np.random.random(n).astype('f4')


@task
async def srun_run(some_int=99):

    logger = get_run_logger()

    logger.info('Running srun business now')
    logger.info(f'Received {some_int=}')

    random.seed(some_int)
    rand_sleep = random.randint(1,10)
    logger.info(f"Sleeping for {rand_sleep}")
    asyncio.sleep(rand_sleep)

    srun_str = (
        f'srun --mpi=pmi2 -N 3 -n 60 '
        f'singularity exec -B $(pwd) {DOCKER_CONTAINER.split("/")[1]}_latest.sif '
        f'./mpi_hello_world'
    )

    logger.info(srun_str)

    #return (f"I am returning now, and here is {srun_str=} "
    #       f" amd the sleep was {rand_sleep=} "
    #)

    result = subprocess.run(
        srun_str,
        shell=True,
        capture_output=True,
        text=True
    )

    logger.info('Finished running the subprocess')

    logger.info(result.stderr)
    logger.info(result.stdout)

    logger.info(result.args)

    #subprocess.run(
    #    "sleep 5", shell=True
    #)

    return ( 
        f"I am returning {some_int} and I was asleep for {rand_sleep} seconds \n"
        f"{result.stdout} \n"
        f"{result.stderr} \n"
    )

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
                    f'-N 3',
                    '-n 60',
                    # '--ntasks-per-node 20'
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
        srun_result = await srun_run.submit(some_int)
        
        (srun_result, *_) = await sleeper(srun_result)

        return srun_result

    return srun_flow

#@flow(
#    task_runner=DaskTaskRunner(
#        cluster_kwargs={
#            "n_workers": 1, 
#            "resources": {"process": 2, "threads_per_worker": 10}, 
#            "local_directory": os.getcwd()
#        },
#    )
#)
@flow
async def main():
    """The main flow with a set of dummy operaitons
    """
    # Define the work
    shape = (600,900)
    a = make_random_array.submit(shape)
    
    print("Running a small collection of subflows...")

    # Database locks can still happen -- poor little sqlite
    collection = await asyncio.gather( 
        *[make_subflow(f"Some other name {i}", i)(i) for i in range(4)]
    )
    print_result_collection(collection)
    


if __name__ == "__main__":
    
    asyncio.run(main())

