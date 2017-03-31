## packjobs

Pack multiple small jobs into large queue jobs

This can be thought of as a highly simplified, standalone, folder (instead of db) based version of https://pythonhosted.org/FireWorks/multi_job.html

### How it works

This merely generates a queue job script and a (mpi-aware) python script

- An outer mpirun in the queue job script places job launchers in the correct nodes

- An Inner mpirun in the job launchers run the application inside each node

The "trick" here is simply to make the queue treat the inner mpi processes as if they were openmp threads of the outer mpi processes (currently for PBS queues)

### How to use

Run `./packjobs.py -h` to see all the command line options

Test run with e.g. 2 nodes, 12 procs per job, 2*24/12=4 simultaneous jobs, 1 hour:

```bash
$ ./packjobs.py -i jobs_folder -r vasp_std -m VASP --nodes 2 --cpn 24 --ppj 12 --time 1
```

Production run with e.g. 50 nodes, 4 procs per job, 50*24/4=300 simultaneous jobs, 24 hours:

```bash
$ ./packjobs.py -i jobs_folder -r vasp_std -m VASP --nodes 50 --cpn 24 --ppj 4 --time 24
```

### Limitations

- If subfolders are added to the job folder after the launchers start running, the new subfolders will not be considered, although this may change in the future

- However, this script can be run multiple times on the same job folder, without duplications (the script tags each subfolder as "running" or "done")

- After a queue job is killed or expires, you may need to clean any "running" tags with `--clean running`
