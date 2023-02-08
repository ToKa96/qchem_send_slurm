#!/opt/bwhpc/common/devel/python/3.8.3/bin/python3
import os
import sys
import argparse
import configparser
import datetime

from subprocess import run
from math import ceil
from dataclasses import dataclass, field


jobscript_main_template = '''
SUBMIT_HOST=$SLURM_SUBMIT_HOST
SUBMIT_WORKDIR=$SLURM_SUBMIT_DIR
JOBID=$SLURM_JOB_ID
JOBNAME=$SLURM_JOB_NAME
QUEUE=$SLURM_JOB_PARTITION
O_HOME=$HOME
NODES=$(< $SLURM_JOB_NODELIST)
NODES_UNIQUE=$(echo "$NODES" | sort -u)
RETURN_VALUE=0
NODE_WORKDIR=$SCRATCH
NODE_SCRATCHDIR=$TMPDIR
#
###################################
#
 print_info() {
    echo ------------------------------------------------------
    echo "Job is running on nodes"
    echo "$NODES" | sed 's/^/    /g'
    echo ------------------------------------------------------
    echo qsys: job was submitted from $SUBMIT_HOST
    echo qsys: executing queue is $QUEUE
    echo qsys: original working directory is $SUBMIT_WORKDIR
    echo qsys: job identifier is $JOBID
    echo qsys: job name is $JOBNAME
    echo ------------------------------------------------------
    echo
}

stage_in() {
    rm -f "$SUBMIT_WORKDIR/job_not_successful"

    echo "Calculation working directory: $NODE_WORKDIR"
    echo "            scratch directory: $NODE_SCRATCHDIR"

    cd $NODE_WORKDIR

    echo
    echo ------------------------------------------------------
    echo
}

stage_out() {
    if [ "$RETURN_VALUE" != "0" ]; then
        touch "$SUBMIT_WORKDIR/job_not_successful"
    fi

    echo
    echo ------------------------------------------------------
    echo

    echo "Final files in $SUBMIT_WORKDIR:"
    (
        cd $SUBMIT_WORKDIR
        ls -l | sed 's/^/    /g'
    )

    echo

    if echo "$NODE_SCRATCHDIR"/* | grep -q "$NODE_SCRATCHDIR/\*$"; then
        # no files in scratchdir:
        du -shc * | sed 's/^/    /g'
    else
        du -shc * "$NODE_SCRATCHDIR"/* | sed 's/^/    /g'
    fi

    echo
}

handle_error() {
    # Make sure this function is only called once
    # and not once for each parallel process
    trap ':' 2 9 15

    echo
    echo "#######################################"
    echo "#-- Early termination signal caught --#"
    echo "#######################################"
    echo
    error_hooks
    stage_out
}
'''
jobscript_main02_template = '''
payload_hooks() {{
:
if [ -r "$SLURM_SUBMIT_DIR/{infile}.in" ]; then 
    CPARGS="--dereference" 
    [ -d "$SLURM_SUBMIT_DIR/{infile}.in" ] && CPARGS="--recursive"
    DIR=$(dirname "{infile}.in")
    mkdir -p "$NODE_WORKDIR/$DIR"
    cp $CPARGS "$SLURM_SUBMIT_DIR/{infile}.in" "$NODE_WORKDIR/$DIR"
fi

export QCSCRATCH="$NODE_SCRATCHDIR"
{qchem_version_path} -slurm -nt {ncpus} "{infile}.in" "{infile}.out"
RETURN_VALUE=$?

# check if job terminated successfully
if ! tail -n 30 "{infile}.out" | grep -q "Thank you very much for using Q-Chem.  Have a nice day."; then
    RETURN_VALUE=1
fi

if [ -r "$NODE_WORKDIR/{infile}.out" ]; then 
    CPARGS="--dereference" 
    [ -d "$NODE_WORKDIR/{infile}.out" ] && CPARGS="--recursive"
    DIR=$(dirname "{infile}.out")
    mkdir -p "$SLURM_SUBMIT_DIR/$DIR"
    cp $CPARGS "$NODE_WORKDIR/{infile}.out" "$SLURM_SUBMIT_DIR/$DIR"
fi
if [ -r "$NODE_WORKDIR/{infile}.in.fchk" ]; then 
    CPARGS="--dereference" 
    [ -d "$NODE_WORKDIR/{infile}.in.fchk" ] && CPARGS="--recursive"
    DIR=$(dirname "{infile}.in.fchk")
    mkdir -p "$SLURM_SUBMIT_DIR/$DIR"
    cp $CPARGS "$NODE_WORKDIR/{infile}.in.fchk" "$SLURM_SUBMIT_DIR/$DIR"
fi
if [ -r "$NODE_WORKDIR/plots" ]; then 
    CPARGS="--dereference" 
    [ -d "$NODE_WORKDIR/plots" ] && CPARGS="--recursive"
    DIR=$(dirname "plots")
    mkdir -p "$SLURM_SUBMIT_DIR/$DIR"
    cp $CPARGS "$NODE_WORKDIR/plots" "$SLURM_SUBMIT_DIR/$DIR"
fi
if [ -r "$NODE_WORKDIR/{infile}.out.plots" ]; then 
    CPARGS="--dereference" 
    [ -d "$NODE_WORKDIR/{infile}.out.plots" ] && CPARGS="--recursive"
    DIR=$(dirname "{infile}.out.plots")
    mkdir -p "$SLURM_SUBMIT_DIR/$DIR"
    cp $CPARGS "$NODE_WORKDIR/{infile}.out.plots" "$SLURM_SUBMIT_DIR/$DIR"
fi


}}

error_hooks() {{
:
if [ -r "$NODE_WORKDIR/{infile}.out" ]; then 
    CPARGS="--dereference" 
    [ -d "$NODE_WORKDIR/{infile}.out" ] && CPARGS="--recursive"
    DIR=$(dirname "{infile}.out")
    mkdir -p "$SLURM_SUBMIT_DIR/$DIR"
    cp $CPARGS "$NODE_WORKDIR/{infile}.out" "$SLURM_SUBMIT_DIR/$DIR"
fi

}}
'''

jobscript_foot_template = '''
###################################
#
# Run the stuff:

print_info
stage_in

echo $(pwd)
for f in $(pwd)
do
    echo $f
done
# If catch signals 2 9 15, run this function:
trap 'handle_error' 2 9 15

payload_hooks
stage_out
exit $RETURN_VALUE
'''

parser_epilog ="""
This script uses keywords from the QChem input file to generate the slurm jobscript.
Two types of lines from the input file are evaluated: 
    - lines containing 'qsys' (case insensitive)
    - lines from the $rem section

    QSYS:
        Structure of the line
        {whatever} qsys <key> = <value>
        {whatever} qsys <key> <value>

        Implemented keywords and formats

        sbatch key      qsys key    formats
        --time          walltime    dd-HH:MM:SS
                        wt          HH:MM:SS
                        time
        --mem           mem         <number>[Unit mb/gb/tb, the b is optional]
                        memory
        --gres=scratch  scratch     <number>[Unit mb/gb/tb, the b is optional]
        -n (cpu count)  ncpus       integer
                        threads     

    QChem
        within $rem same structure as all rem keywords

        sbatch key      qchem key   formats
        --mem           mem_total   integer (no unit but always in mb)
        -n (cpu count)  threads     integer

    ressources request via the command line (via -l) should overwrite anything specified 
    in the input file. 

** Attention **
Unlike our cluster JUSTUS2 does not automatically assign out of ram scratch space
thus it is advised to request it if your calculations will write significant amount 
of output data as otherwise it will be deducted from your ram limit! 
"""

def _last_not_none(arg: list):
    return next((x for x in reversed(arg) if x is not None), None)


def _timedelta_from_string(string: str):
    time = None
    days, rest = string.split('-')
    hours, minutes, seconds = [int(x) for x in rest.split(':')]

    seconds += 60 * minutes
    seconds += 3600 * hours
    try:
        seconds += 24 * 3600 * int(days)
    except ValueError:
        pass
    # TODO: add further exception handling

    days = seconds // (24 * 3600)
    seconds = seconds - (days * 24 * 3600)
    time = datetime.timedelta(days=days, seconds=seconds)
    return time


class SlurmMemory:

    def __init__(self):
        self._data = [None, None, None]

    def __set__(self, obj, value):
        data = None
        i = 0
        if isinstance(value, str):
            value = value.lower()
            if value.endswith('m') or value.endswith('mb'):
                data = int(''.join(c for c in value if (c.isdigit() or c == '.')))
            elif value.endswith('gb') or value.endswith('g'):
                data = int(float(''.join(c for c in value if (c.isdigit() or c == '.'))) * 1024)
            elif value.endswith('tb') or value.endswith('t'):
                data = int(float(''.join(c for c in value if (c.isdigit() or c == '.'))) * 1024 * 1024)
            else:
                print(f"** Warning Unusual memory string encountered: {value}")
        elif isinstance(value, float) or isinstance(value, int):
            data = int(value)
        else:
            if value is not None:
                print(f"** Warning Unusual memory encountered: {value}")

        for i, val in enumerate(self._data):
            if val is None:
                self._data[i] = data
                break

        if i >= 1:
            print('** Warning ** QSYS/CMD overwrites qchem variable Memory')

    def __get__(self, obj, type):
        ret = None
        if obj is None:
            ret = _last_not_none(self._data)
        else:
            ret = _last_not_none(getattr(self, "_data"))

        if ret is None:
            return ret
        elif ret % (1048576) == 0:
            return f'{ret // 1048576}T'
        elif ret % 1024 == 0:
            return f"{ret // 1024}G"
        else:
            return f"{ret}M"


class SlurmScratch:

    def __init__(self):
        self._data = [None, None, None]

    def __set__(self, obj, value):
        data = None
        i = 0
        if isinstance(value, str):
            value = value.lower()
            if value.endswith('m') or value.endswith('mb'):
                data = int(''.join(c for c in value if (c.isdigit() or c == '.')))
            elif value.endswith('gb') or value.endswith('g'):
                data = int(float(''.join(c for c in value if (c.isdigit() or c == '.'))) * 1024)
            elif value.endswith('tb') or value.endswith('t'):
                data = int(float(''.join(c for c in value if (c.isdigit() or c == '.'))) * 1024 * 1024)
            else:
                print(f"** Warning Unusual scratch string encountered: {value}")
        elif isinstance(value, float) or isinstance(value, int):
            data = int(value)
        else:
            if value is not None:
                print(f"** Warning Unusual scratch encountered: {value}")

        for i, val in enumerate(self._data):
            if val is None:
                self._data[i] = data
                break

        if i >= 1:
            print('** Warning ** QSYS/CMD overwrites qchem variable Scratch')
    
    def __get__(self, obj, type):
        ret = None
        if obj is None:
            ret = _last_not_none(self._data)
        else:
            ret = _last_not_none(getattr(self, "_data"))

        if ret is None:
            return ret
        else:
            return f'{ceil(ret / 1024)}'


class SlurmTime:
    def __init__(self):
        self._data = None

    def __set__(self, obj, value):
        if isinstance(value, str):
            self._data = _timedelta_from_string(value)
        elif isinstance(value, datetime.datetime):
            self._data = datetime.timedelta(
                days=value.day,
                hours=value.hour,
                minutes=value.minute,
                seconds=value.second)
        elif isinstance(value, datetime.timedelta):
            self._data = value
        else:
            pass

    def __get__(self, obj, type):
        if obj is None:
            timedelta = self._data
        else:
            timedelta = getattr(self, '_data')

        if timedelta is None:
            return None
        seconds = int(timedelta.total_seconds())
        hours = seconds // 3600
        minutes = (seconds // 60) - hours * 60
        seconds = seconds - 3600 * hours - 60 * minutes
        days = hours // 24
        hours = hours - days * 24
        return f"{days:02d}-{hours:02d}:{minutes:02d}:{seconds:02d}"


@dataclass
class JobData:
    # init stuff
    mail: str
    mail_type: str
    qchem_version_path: str = ''

    mem: SlurmMemory = SlurmMemory()

    # data from cmd/.in file
    jobname: str = field(init=False)
    scratch: SlurmScratch = SlurmScratch()
    time: SlurmTime = SlurmTime()
    ncpus: int = field(init=False)

    def create_header(self):
        ret = '#!/bin/bash\n'
        ret += f'#SBATCH --job-name={self.jobname}\n'
        ret += '#SBATCH --nodes=1\n'

        if self.mem is not None:
            ret += f'#SBATCH --mem={self.mem}\n'
        if self.ncpus is not None:
            ret += f'#SBATCH -n {self.ncpus}\n'
        if self.scratch is not None:
            ret += f"#SBATCH --gres=scratch:{self.scratch}\n"
        if self.time is not None:
            ret += f"#SBATCH --time={self.time}\n"
        if self.mail:
            ret += f"#SBATCH --mail-user={self.mail}\n"
            if self.mail_type:
                ret += f"#SBATCH --mail-type={self.mail_type.replace(' ','')}\n"

        ret += '#SBATCH --output="%x.o%j"\n'

        return ret

    def check_data(self):
        if self.time is None:
            print('** Warning ** no walltime set')
        if self.mem is None:
            print('** Warning ** no ram memory set')
        if self.scratch is None:
            print('** Warning ** no scratch space set')


def load_config():
    """TODO: Docstring for load_config.
    :returns: TODO

    """
    standard_path = f"{os.path.expanduser('~')}/.config/qchem_send_slurm.conf"
    config = configparser.ConfigParser()
    try:
        if not os.path.isfile(standard_path):
            raise FileNotFoundError
        config.read(standard_path)
    except FileNotFoundError:
        print('No config File found')
        inp = input('Would you like to create one?\n')
        if inp.lower().startswith('y'):
            write_config()
            load_config()
        else:
            print('** Warning this script might show unexpected behaviour without such a config file!')

    return config


def write_config(path=''):
    """TODO: Docstring for write_config.
    :returns: TODO

    """
    version_path = '/lustre/work/ws/ws1/hd_ie450-dreuw_qchem/versions/'
    standard_path =os.path.join(os.path.expanduser('~'), '.config/qchem_send_slurm.conf') 
    default_version = ''
    mail = ''
    mail_type = ['END', 'FAIL']

    print('creating new config file')
    if path == '':
        inp = input(f"using standard path (heavily recommended) [y/n]? ({os.path.expanduser('~')}/.config/qchem_send_slurm.conf)\n")
        if inp.lower().startswith('n'):
            path = input('Please enter a valid path for the config file\n')
            if not os.path.basename(path):
                path += 'qchem_send_slurm.conf'
            print('** Warning ** changing the default path might lead to unexpected behaviour and require changes to the script to work.')
        else:
            print('using default path for config file')
            path = f"{os.path.expanduser('~')}/.config/qchem_send_slurm.conf"
    else:
        if path != standard_path:
            print('** Warning ** changing the default path might lead to unexpected behaviour and require changes to the script to work.')

    os.makedirs(os.path.dirname(path), exist_ok=True)

    config = configparser.ConfigParser()

    inp = input(f'Please enter the path to your qchem version script directory for default press enter (Default: {version_path})\n')
    if inp:
        if not os.path.exists(inp):
            print(f'** Warning ** {inp} does not exist!')
        version_path = inp

    config['PATHS'] = {'qchem_version_dir': version_path}

    if os.path.exists(version_path):
        if os.path.isdir(version_path):
            print('Following version scripts found:')
            versions = []
            for f in os.listdir(version_path):
                if os.path.isfile(f):
                    vscript = os.path.basename(f)
                    print(vscript)
                    versions.append(vscript)

            inp = input('If you wish to set one of these as default please enter its name?\n')

            if inp:
                if inp in versions:
                    default_version = version_path + inp
                else:
                    for version in versions:
                        if inp in version:
                            default_version = version_path + version
                if default_version == '':
                    print('** Warning version script does not exist')
            else:
                print('No default qchem version set')

        if os.path.isfile(version_path):
            print(f'{version_path} is a file using it as default qchem version script')
            default_version = version_path

    config['PATHS'] = {'qchem_version': default_version}

    mail = input('Please enter mail address to use for slurm notifications?\n')
    print(f'Using {mail} as mail address for slurm')

    inp = input(f'When do you wish to receive mails if you do NOT wish to receive any emails enter NONE? (Default {mail_type}, Possible Values: BEGIN, END, FAIL, INVALID_DEPEND, REQUEUE, STAGE_OUT, TIME_LIMIT, TIME_LIMIT_90, TIME_LIMIT_80 and TIME_LIMIT_50)\n')

    if inp.capitalize().startswith('NO'):
        mail_type = []
    elif inp == '':
        print('using default for mail types')
    else:
        mail_type = [mtype for mtype in inp.split(',')]

    config['MAIL'] = {'mail': mail, 'mail-type': ', '.join(mail_type)}

    with open(path, 'w') as configfile:
        config.write(configfile)


def read_qchem(path, data: JobData):
    key_mapping = {
        'ncpus': ['threads'],
        'mem': ['mem_total'],
    }
    qchem_keys = ['threads', 'mem_total']
    qchem = {}
    with open(path) as qin:
        rem_section = False
        for line in qin:
            line = line.lower()

            if '$end' in line:
                rem_section = False

            if rem_section:
                line = line.replace('=', ' ')
                splits = line.split()
                key, value, *_ = splits
                if key in qchem_keys:
                    qchem[key] = value

            if '$rem' in line:
                rem_section = True

    try:
        qchem['mem_total'] = int(qchem['mem_total']) * 1.05
    except KeyError:
        pass

    # print(qchem)
    # qchem_keys = set(qchem.keys())
    # i = 0
    for key, value in key_mapping.items():
        value = set(value)
        intersec = value.intersection(qchem_keys)
        if intersec:
            qs_key = intersec.pop()
            setattr(data, key, qchem[qs_key])


def read_qsys(path, data: JobData):
    key_mapping = {
        'time': ['walltime', 'wt', 'time'],
        'mem': ['memory', 'mem'],
        'scratch': ['scratch'],
        'ncpus': ['threads', 'ncpus'],
    }
    qsys = {}
    with open(path) as qin:
        for line in qin:
            line = line.lower()
            if 'qsys' in line:
                line = line.replace("=", " ")
                splits = line.split('qsys')[-1]
                splits = splits.split()
                # for item in ['=', ':']:
                #     try:
                #         splits.remove(item)
                #     except ValueError:
                #         pass

                if len(splits) == 2:
                    key, value = splits
                    qsys[key] = value
                else:
                    print('** Warning ** Unusual QSYS line detected ignoring it')
                    print(line)
                    print(splits)

    qsys_keys = set(qsys.keys())
    for key, value in key_mapping.items():
        value = set(value)
        intersec = value.intersection(qsys_keys)
        if intersec:
            qs_key = intersec.pop()
            setattr(data, key, qsys[qs_key])


def read_qin(path, data: JobData):
    """TODO: Docstring for read_qin.

    :path: TODO
    :returns: TODO

    """
    read_qchem(path, data)
    read_qsys(path, data)




def choose_version(version_path):
    versions = []
    for f in os.listdir(version_path):
        if os.path.isfile(f):
            vscript = os.path.basename(f)
            print(vscript)
            versions.append(vscript)

    inp = input('If you wish to set one of these as default please enter its name?\n')
    ret = version_path
    for version in versions:
        if inp in version:
            version_path += version

    return ret


def write_jobscript(path, data: JobData):
    """TODO: Docstring for write_jobscript.

    :path: TODO
    :data: TODO
    :returns: TODO

    """
    jspath = os.path.join(os.path.dirname(path), os.path.basename(path).replace('.in', '.sh'))
    infile = os.path.join(os.path.dirname(path), os.path.basename(path).replace('.in', ''))
    print(infile)
    jobscript = ''
    jobscript += data.create_header()
    jobscript += jobscript_main_template
    jobscript += jobscript_main02_template.format(infile=infile, qchem_version_path=data.qchem_version_path, ncpus=data.ncpus)
    jobscript += jobscript_foot_template

    with open(jspath, 'w') as js:
        js.write(jobscript)

    return jspath


def send_job(path, args):
    run(f'sbatch {path} {args}', shell=True)

def cmd_args(argv):
    config = load_config()
    parser = argparse.ArgumentParser(description='A qchem jobscript creaion tool intended for the use on the JUSTUS2 bwhp cluster with Slurm.',
                                    epilog=parser_epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--config', action='store_true', help="rewrite the config file")
    # parser_config.add_argument('-p', '--path', help='sets the path for the config file')
    # parser_jobscript = subparser.add_parser('', help='creates the jobscript')
    parser.add_argument('INFILE', nargs='+', help='the qchem input files for which the jobscripts are to be generated.')
    parser.add_argument('-l', action='append', help='specify resources for SLURM, will be forwarded to sbatch. use its syntax!')
    parser.add_argument('--no-send', action='store_false', help='flag to prevent sending the job to the cluster')
    parser.add_argument('--version', help='give name or the path to a qchem version script.')

    parser.set_defaults(func=main)
    args = parser.parse_args(argv)
    args.func(vars(args), config)

def main(cmd, config):
    infiles = cmd['INFILE']
    sbatch_args = cmd['l']
    no_send = cmd['no_send']
    version = cmd['version']

    if cmd['config']:
        write_config()

    if version is None:
        try:
            version = config['PATHS']['qchem_version']
            if not os.path.isfile(version):
                raise KeyError
        except KeyError:
            version = choose_version(config['PATHS']['qchem_version_path'])

    for fn in infiles:
        jobname = os.path.basename(fn).replace('.in', '')
        mail_user = config['MAIL']['mail']
        mail_type = config['MAIL']['mail-type']
        jd = JobData(
            mail=mail_user,
            mail_type=mail_type,
            qchem_version_path=version,
        )
        jd.jobname = jobname

        read_qin(fn, jd)
        jd.check_data()
        jspath = write_jobscript(fn, jd)

        if no_send:
            send_job(jspath, sbatch_args)


if __name__ == "__main__":
    cmd_args(sys.argv[1:]) 
