## packjobs

Pack multiple small jobs into large queue jobs

### How it works

This merely generates a queue job script and a (mpi-aware) python script

An outer mpirun in the queue job script places job launchers in the correct nodes

An Inner mpirun in the job launchers run the application inside each node

The "trick" here is simply to make the queue treat the inner mpi processes as if 
they were openmp threads of the outer mpi processes (currently for PBS queues)

### How to use

Run `./packjobs.py -h` to see all the command line options

Test with e.g. (2 nodes, 12 procs per job, 2*24/12=4 simultaneous jobs)

```bash
$ ./packjobs.py -i jobs_folder -n 2 -r vasp_std -m VASP --ppj 12 --hours 1
```

Production run with e.g. (50 nodes, 4 procs per job, 50*24/4=300 simultaneous jobs)

```bash
./packjobs.py -i jobs_folder -n 50 -r vasp_std -m VASP --ppj 4 --hours 24
```

### Limitations

If subfolders are added to the job folder after the launchers start running, 
the new subfolders will not be considered, although this may change in the future

However, this script can be run multiple times on the same job folder, 
without duplications (the script tags each subfolder as "running" or "done")

After a queue job is killed or expires, you may need to clean any "running" tags
