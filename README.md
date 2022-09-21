## Prefect2 Example

This is a simple example Prefect (version 2) workflow that is attempting to create a minimum working example of some desirable features intended (required!) to replicate functionality from the ASKAP processing pipeline. Specifically, the heart of this workflow is trying to:
- Spawn tasks running on a SLURM cluster (with multi-node allocation)
- Execute MPI enable application across node boundaries
- Compose workflows from many smaller sub-flows while allowing for concurrency

### MPI application standin

Included is a `c` file that implements a simple MPI application. This will have to be compiled using `mpicc` somehow. If docker is installed this can be done via: 

`docker run -it -v ${PWD}:/project nlknguyen/alpine-mpich mpicc -o kekek mpi_hello_world.c`

The compiled `mpi_hello_world` should be stored alongside the `prefect2_flow.py` file. 

### Environment

I am running this example code on the `Galaxy` system at `Pawsey`. Hence, there are some hardcoded values in this example flow, particularly when trying to spin up the `SLURMCluster`. 

A virtual environment with `python=3.10` should be set up, and the following packages installed via `pip` -- other package managers (`conda` for instance) are pulling down unintended versions. The following should get to a workable environment:

```
conda create -n prefect3.10 python=3.10 --yes
conda activate prefect3.10
pip install numpy
pip install dask prefect
pip install prefect_dask
pip install dask_jobqueue
```

For completeness there is a `package_list.txt` file outlining the installed packages (in case there is anything missed from the above set of `pip` installables). 

