# qchem_send_slurm

A QChem jobscript creation tool intended for the use on the JUSTUS2 bwhp cluster with Slurm.

It uses keywords from the QChem input file to detect the reuired setting for the slurm jobscript.

## Prerequisites

- Python 3.7 or newer (but only tested for 3.8.3 and 3.10)

## Installation

- Clone the repo or just download the qchem_send_slurm.py file
- run the script for the first time and create the config file it requests at the default location
  - alternativley create a config file `$HOME/.config/qchem_send_slurm.conf` by hand (not recommended)
- check the shebang line and whether it points to the correct python interpreter
- add and alias to this script or add it to your `PATH`
  
## Usage

See `qchem_send_slurm --help`
~~~
usage: qchem_send_slurm_v2.py [-h] [-c] [-l L] [--no-send] [--version VERSION] INFILE [INFILE ...]

A qchem jobscript creaion tool intended for the use on the JUSTUS2 bwhp cluster with Slurm.

positional arguments:
  INFILE             the qchem input files for which the jobscripts are to be generated.

optional arguments:
  -h, --help         show this help message and exit
  -c, --config       rewrite the config file
  -l L               specify resources for SLURM, will be forwarded to sbatch. use its syntax!
  --no-send          flag to prevent sending the job to the cluster
  --version VERSION  give name or the path to a qchem version script.

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
~~~
