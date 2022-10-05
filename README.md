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

The important bit is to ensure that `prefect` version 2 (and above) is installed. `prefect` version 1 has a different API that is not compatible with this example code. 

### Prefect Environment Variables

As a users home space is often not available on compute nodes (`quota` limits for instance), a simple helper script has been included to configure some `prefect` settings. This file should be sourced before running the `python prefect2_flow.py` workflow. Specifically, this helper script will:
- Change the database connection string to a local `sqlite` database via `PREFECT_ORION_DATABASE_CONNECTION_URL`
- Change the `prefect` home location via `PREFECT_HOME`

These both are simply set to the current working directly the file is sources from. 

### Running

If the above is carried out, then the workflow should be executable within the `prefect3.10` `conda` environment via:

`python prefect2_flow.py`

## Other thoughts and issues

Below are some items that I wanted to iterate over. 

### Prefect database

A `postgres` database should be used the moment more than a handful of flows are running. The normal `sqlite` database relies on file locks for multi-user read/write. Simply, the application will crash as multiple concurrent connections are not able to acquire exclusive write access in time. In `postgres_database.sh` are some steps to create a compatible `postgres` database with a container. *Note* though that the `PREFECT_ORION_DATABASE_CONNECTION_URL` should contain the IP address of the compute node hosting the container -- a localhost address (which is what is currently supplied) is not appropriate. 

### SLURMCluster vs ScaleSLURMCluster

This will likely become a little more confusing as more experimentation is carried out. 

In short, these task runners need to be fired up when the flow is invoked -- not when the larger script is started. For the `SLURMCluster` objects, some care needs to be taken to ensure that the `scale` method is invoked. This will actually submit the generated `SLURM` jobscript through to the `SLURM` schedular. 

The `ScaleSLURMCluster` is intended to be the same as `SLURMCluster`, except that the `scale` method is invoked during the initialisation of the class instance. Why? It did not appear to be a case that the `scale` was automatically invoked by the `DaskTaskRunner` as it was being created. 

However, upon reading the documentation for `dask_jobqueue.SLURMCluster` a little more closely, there is the `n_workers` argument. In practise, setting this to `1` via the `cluster_kwargs` of `DaskTaskRunner` does seem to give the behaviour expected. It remains to be seen how this will properly interact with the necessary changes that have yet to be made (in this test enviornment) with the `dask_jobqueue.SLURMCluster` to enable multi-node requests. Ensure multi-node requests are possible is essential to enable the `mpi` compiled ASKAP applications to run as efficently as possible. How the use of `SLURMCluster` will behave when initialised with the `n_workers=1` is not clear to me (although it probably should be at this point). 

### Schedular address becoming exhausted

A new dask dashboard and schedular are started for each `SLURMCluster` instance that is created when using its default parameters. Through experimentation it was found that when ~35 sub-flows were started to run concurrently that these schedulars would start to try to use the same port when creating their schedular address. This would, in turn, kill the pipeline with an `[Error 98] Address in use` error. This has been worked around by explicitly setting a (presumably) unused port number using:

`                scheduler_options=dict(dashboard_address=f':{32120+count}'),`

via the `cluster_kwargs` provided to the `DaskTaskRunner`. 

### Connection pool to database

A sqlalchemy `QueuePool` error will be raised if too many sub-flows using a `SLURMCluster` are created too quickly. Essemtially, the database engine can not keep up with the number of incoming transactions, and the limited set of open connections which are persistent and intended for reuse can not be shared. By default, the number of open connections is 5, with an additional 10 allowed.

Internal to `prefect` this can be worked around by adding

```
kwargs["pool_size"] = 13
kwargs["max_overflow"] = 30
```
to the `AsyncPostgresConfiguration.engine` method. 

This seems to become a problem when in excess of 140 concurrent sub-flows are created. In implementing the above work around a new issue is raised. 

Provided that fewer than 80 `SLURMCluster` based sub-flows are executed in a concurrent setting I think this is (and the next) error are unlikely to be a show stopper. 

### TimeOutError

It seems that from the above that when there are too many corountines running an `asyncio.exceptions.TimeoutError` will be raised. Likely, it seems to be a case that some coroutine has waited for too long in the event loop, deem old, and killed. 

Whether this is an error unto itself or it is simple a result from the previous change to enlarge the `QueuePool` characteristics remains to be understood. 

Provided that fewer than 80 `SLURMCluster` based sub-flows are executed in a concurrent setting I think this is (and the previous) error are unlikely to be a show stopper. 

