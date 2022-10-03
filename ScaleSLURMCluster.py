from dask_jobqueue import SLURMCluster

class ScaleSLURMCluster(SLURMCluster):
    def __init__(self, *args, **kwargs):
        print('Running Super')
        super().__init__(*args, **kwargs)
        
        print("Scaling using self.scale")
        self.scale(1)
