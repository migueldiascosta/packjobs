#!/usr/bin/env python
import os
import sys
import argparse

from glob import glob
from textwrap import dedent
from collections import namedtuple

__version__ = 20200612

__doc__ = """Pack multiple small jobs into large queue jobs

* How it works

  * The script merely generates a queue job script and a (mpi-aware) python script

  * An outer mpirun in the queue job script places job launchers in the correct nodes

  * An Inner mpirun in the job launchers run the application inside each node

  * The "trick" here is simply to make the queue treat the inner mpi processes
    as if they were openmp threads of the outer mpi processes

* How to use

  * Run ./packjobs.py -h to see all the command line options

  * Test run with e.g. 2 nodes, 12 procs per job, 2*24/12=4 simultaneous jobs, 1 hour:
    ./packjobs.py -i jobs_folder -r vasp_std -m VASP --nodes 2 --cpn 24 --ppj 12 --time 1

  * Production run with e.g. 50 nodes, 4 procs per job, 50*24/4=300 simultaneous jobs, 24 hours:
    ./packjobs.py -i jobs_folder -r vasp_std -m VASP --nodes 50 --cpn 24 --ppj 4 --time 24

* Limitations

  * If subfolders are added to the job folder after the launchers start running,
    the new subfolders will not be considered, although this may change in the future

  * However, this script can be run multiple times on the same job folder,
    without duplications (the script tags each subfolder as "running" or "done")

  * After a queue job is killed or expires, you may need to clean any "running" tags
    with "--clean running"
"""


def parse_arguments():
    """Use argparse to get parameters from the command line"""

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-V', '--version', action='version', version='%%(prog)s %s' % __version__)

    parser.add_argument('-i', '--input', dest='folder', type=str,
                        help="folder containing job folders (mandatory)", required=True)

    parser.add_argument('-r', '--run', dest='job_cmd', type=str,
                        help="job command (e.g. vasp_std) (mandatory)", required=True)

    parser.add_argument('-m', '--mod', dest='job_mod', type=str,
                        help="app module (e.g. VASP) (mandatory)", required=True)

    parser.add_argument('-p', '--python-mod', dest='python_mod', type=str,
                        help="python module (e.g. Python)", default='Python')

    parser.add_argument('-n', '--nodes', dest='nodes', type=int,
                        help="number of nodes (mandatory)", required=True)

    parser.add_argument('-t', '--time', dest='hours', type=int, default=1,
                        help="number of hours for qjob (default: 1)")

    parser.add_argument('-q', '--queue', dest='queue', type=str, default='normal',
                        help="name of batch queue for qjob (default: normal)")

    parser.add_argument('-b', '--batch', dest='batch', type=str, default='pbs',
                        help="name of batch system for qjob, currently pbs or lsf (default: pbs)")

    parser.add_argument('--cpn', '--cores-per-node', dest='cores_per_node', type=int, default=24,
                        help="number of cores per node (default: 24)")

    parser.add_argument('--mpn', '--memory-per-node', dest='memory_per_node', type=int, default=96,
                        help="memory per node, in GB (default: 96)")

    parser.add_argument('--ppj', '--procs-per-job', dest='procs_per_job', type=int, default=1,
                        help="number of mpi processes per job (default: 1)")

    parser.add_argument('-d', '--dry-run', dest='dry', action='store_true', default=False,
                        help="don't submit, only create scripts (default: false)")

    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False,
                        help="don't ask for confirmation when deleting files (default: false)")

    parser.add_argument('-c', '--clean', action='append', default=[],
                        choices=['done', 'running', 'scripts', 'all'],
                        help='delete previously generated file (default: false)')

    args = parser.parse_args()

    if 'all' in args.clean:
        args.clean.append('done')
        args.clean.append('running')
        args.clean.append('scripts')

    if not os.path.isdir(args.folder):
        print("\n Folder %s does not exist, exiting" % args.folder)
        sys.exit(1)

    if args.cores_per_node % args.procs_per_job != 0:
        print("\n cores_per_node must be divisible by procs_per_job")
        sys.exit(1)

    args.jobs_per_node = int(args.cores_per_node/args.procs_per_job)

    print("\n Requesting %s nodes, %s cores per node, using %s processes per job" %
          (args.nodes, args.cores_per_node, args.procs_per_job))

    print("\n This means %s jobs per node, %s simultaneous jobs at any given time\n" %
          (args.jobs_per_node, args.jobs_per_node*args.nodes))

    return args


class PackJobs:
    __doc__ = __doc__

    def __init__(self, **kwargs):
        """Takes keywords and maps them explicitly to class attributes"""

        self.nodes = kwargs.pop('nodes')
        self.folder = kwargs.pop('folder')
        self.job_cmd = kwargs.pop('job_cmd')
        self.job_mod = kwargs.pop('job_mod')
        self.python_mod = kwargs.pop('python_mod')

        self.hours = kwargs.pop('hours', 1)
        self.queue = kwargs.pop('queue', 'normal')
        self.batch = kwargs.pop('batch', 'pbs')
        self.cores_per_node = kwargs.pop('cores_per_node', 24)
        self.memory_per_node = kwargs.pop('memory_per_node', 96)
        self.procs_per_job = kwargs.pop('procs_per_job', 1)
        self.jobs_per_node = kwargs.pop('jobs_per_node', int(self.cores_per_node/self.procs_per_job))

        self.dry = kwargs.pop('dry', False)
        self.force = kwargs.pop('force', False)
        self.clean = kwargs.pop('clean', False)

        if len(kwargs.keys()) > 0:
            self.log("don't know what to do with remaining arguments %s" % str(kwargs))

        if self.batch == 'lsf':
            self.qjob_script_template = self.qjob_lsf_template
            self.qjob_sub_cmd = 'bsub <'
            self.qjob_stat_cmd = 'bjobs'
        else:
            self.qjob_script_template = self.qjob_pbs_template
            self.qjob_sub_cmd = 'qsub'
            self.qjob_stat_cmd = 'qstat'

        self.mpirun_job = ''
        self.qjob_script_path = ''

    def run(self):
        """Run all steps (clean, read_jobs, write_scripts, submit_jobs)"""

        self.clean_files()
        self.read_jobs()
        self.write_scripts()
        self.submit_jobs()

    def clean_files(self):
        """Clean previously generated files if requested applicable"""

        if 'all' in self.clean:
            self.log("Warning: Deleting all files (but not subfolders) in %s" % self.folder)
            if self.confirm():
                for f in glob(os.path.join(self.folder, '*')):
                    if os.path.isfile(f):
                        os.remove(f)
        else:
            if 'scripts' in self.clean:
                self.log("Warning: Deleting any previously generated qjob and worker scripts")
                if self.confirm():
                    for qjob_script in glob(os.path.join(self.folder, 'qjob.script')):
                        os.remove(qjob_script)
                    for worker_py in glob(os.path.join(self.folder, 'worker*.py')):
                        os.remove(worker_py)

    def read_jobs(self):
        """Look for jobs in job folder"""

        self.log("Reading from folder %s" % self.folder)

        Job = namedtuple('Job', ['folder', 'running', 'done'])

        all_jobs = sorted([Job(subfolder,
                               os.path.isfile(os.path.join(self.folder, subfolder, 'running')),
                               os.path.isfile(os.path.join(self.folder, subfolder, 'done')))
                           for subfolder in os.listdir(self.folder)
                           if os.path.isdir(os.path.join(self.folder, subfolder))])

        running_jobs = [job.folder for job in all_jobs if job.running]
        finished_jobs = [job.folder for job in all_jobs if job.done]
        unstarted_jobs = [job.folder for job in all_jobs if not job.running and not job.done]

        self.log("Found %s jobs, %s of them currently running, %s of them done" %
                 (len(all_jobs), len(running_jobs), len(finished_jobs)))

        jobs = unstarted_jobs

        if 'running' in self.clean:
            self.log("Warning: Forcing execution of jobs tagged as running")
            if self.confirm():
                for job in running_jobs:
                    os.remove(os.path.join(self.folder, job, 'running'))
                jobs.extend(running_jobs)

        if 'done' in self.clean:
            self.log("Warning: Forcing execution of jobs tagged as done")
            if self.confirm():
                for job in finished_jobs:
                    os.remove(os.path.join(self.folder, job, 'done'))
                jobs.extend(finished_jobs)

        if len(jobs) > 0:
            self.log("Adding %s jobs" % len(jobs))

            if len(jobs) < self.jobs_per_node*self.nodes:
                print("WARNING: with these jobs and parameters, some cores will be idle")
        else:
            self.log("No jobs left to run, exiting. You may want to use clean done and/or clean running")
            sys.exit(1)

    def write_scripts(self):
        """Write queue job and launcher scripts according to given parameters"""

        self.mpirun_job = "mpirun -host $(hostname) -np %s %s > out 2> error" % \
            (self.procs_per_job, self.job_cmd)

        var_dict = {
            'folder': self.folder,
            'job_cmd': self.mpirun_job,
            'job_mod': self.job_mod,
            'python_mod': self.python_mod,
            'nnodes': self.nodes,
            'cpn': self.cores_per_node,
            'mpn': self.memory_per_node,
            'sjpn': self.jobs_per_node,
            'ppj': self.procs_per_job,
            'hours': self.hours,
            'queue': self.queue,
            'njobs': self.jobs_per_node*self.nodes,
            'nslots': int(self.nodes*self.cores_per_node),
            }

        existing_workers = glob(os.path.join(self.folder, 'worker*.py'))

        worker = 'worker%s' % (len(existing_workers)+1)
        worker_py = worker + '.py'

        var_dict['worker'] = worker
        var_dict['worker_py'] = worker_py

        worker_py_path = os.path.join(self.folder, worker_py)

        if not self.dry:
            self.log("Writing %s" % worker_py_path)
            f = open(worker_py_path, 'w')
            f.write(dedent(self.worker_script_template % var_dict))
            f.close()
            os.system("chmod +x %s" % worker_py_path)

        existing_qjobs = glob(os.path.join(self.folder, 'qjob*.script'))

        self.qjob_script_path = os.path.join(self.folder, 'qjob%s.script' % (len(existing_qjobs) + 1))

        if not self.dry:
            self.log("Writing %s" % self.qjob_script_path)
            f = open(self.qjob_script_path, 'w')
            f.write(dedent(self.qjob_script_template % var_dict))
            f.close()

    def submit_jobs(self):
        """Submit queue job"""

        if not self.dry:
            self.log("Submitting %s" % self.qjob_script_path)
            folder, script = os.path.split(self.qjob_script_path)
            os.system("cd %s; %s %s" % (folder, self.qjob_sub_cmd, script))

        sys.stdout.write("\n")
        os.system(self.qjob_stat_cmd)

    def log(self, msg):
        """Print formatted log message"""

        output = " "
        if self.dry:
            output += "(dry run) "
        output += msg
        output += "\n\n"
        sys.stdout.write(output)

    def confirm(self, prompt=None, default_yes=True, abort_no=False):
        """Prompt for confirmation, optionally aborting execution"""

        if self.dry:
            return False

        if self.force:
            return True

        if prompt is None:
            prompt = 'Proceed?'

        if default_yes:
            prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
        else:
            prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

        while True:
            ask = getattr(__builtins__, 'raw_input', input)
            ans = ask(prompt)
            if not ans:
                return default_yes
            if ans not in ['y', 'Y', 'n', 'N']:
                print('please enter y or n.')
                continue
            if ans in ('Y', 'y'):
                return True
            if ans in ('N', 'n'):
                if abort_no:
                    sys.exit(1)
                else:
                    return False

    qjob_pbs_template = """\
        #!/bin/bash
        #PBS -N %(worker)s
        #PBS -l select=%(nnodes)s:ncpus=%(cpn)s:mpiprocs=%(sjpn)s:ompthreads=%(ppj)s:mem=%(mpn)sGB
        #PBS -l walltime=%(hours)s:00:00
        #PBS -j oe
        #PBS -q %(queue)s

        cd $PBS_O_WORKDIR

        module purge
        module load %(python_mod)s %(job_mod)s

        # this mpirun, combined with mpiprocs and ompthreads queue settings,
        # starts job launchers in the correct nodes
        mpirun -np %(njobs)s ./%(worker_py)s
        """

    qjob_lsf_template = """\
        #!/bin/bash
        #BSUB -J %(worker)s
        #BSUB -n %(nslots)s
        #BSUB -q %(queue)s
        #BSUB -R \"span[ptile=%(sjpn)s]\"
        #BSUB -R \"rusage[mem=%(mpn)s000]\"
        #BSUB -W %(hours)s:00
        #BSUB -eo
        #BSUB -x

        module purge
        module load %(python_mod)s %(job_mod)s

        # this mpirun, combined with the span[ptile] queue setting,
        # starts job launchers in the correct nodes
        mpirun -np %(njobs)s ./%(worker_py)s
        """

    worker_script_template = """\
        #!/usr/bin/env python
        import os
        import sys
        import glob
        import argparse

        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

        jobs = sorted([d for d in os.listdir(os.getcwd()) if os.path.isdir(d)])

        j = rank

        name = os.path.splitext(os.path.basename(sys.argv[0]))[0]

        status = open('status.' + name + '.rank' + str(rank), 'w')

        while j < len(jobs):
            running = os.path.isfile(os.path.join(jobs[j], 'running'))
            done = os.path.isfile(os.path.join(jobs[j], 'done'))
            if not running and not done:
                status.write("running " + jobs[j] + "\\n")
                status.flush()
                os.chdir(jobs[j])
                open('running', 'w').close()
                error = os.system("%(job_cmd)s")
                if not error:
                    os.remove('running')
                    open('done', 'w').close()
                    status.write(jobs[j] +  " done\\n")
                    status.flush()
                else:
                    status.write(jobs[j] + " failed\\n")
                    status.flush()
                os.chdir('..')
            else:
                status.write(jobs[j] + " skipped\\n")
                status.flush()
            j += size

        status.write("finished\\n")
        status.close()
        """


if __name__ == "__main__":

    args_dict = vars(parse_arguments())

    p = PackJobs(**args_dict)

    p.run()
