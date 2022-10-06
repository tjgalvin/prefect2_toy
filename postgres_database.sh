#!/bin/bash -l

help="This script will configure the prefect environment, and if requested start the 
postgres server necessary.

Options:
    -s  - will attempt to start the postgres server from a singularity container
    -h  - will print this help page

Usage:
postgres_database.sh [-s | -h]
"

START_POSTGRES=1

while getopts 'sh' arg; do 
    case $arg in
    s)
        echo "Will attempt to start postgres server"
        START_POSTGRES=0
        ;;
    *)
        echo "$help" 
        exit 1
        ;;
    esac
done

# Now set up some postgres values
export POSTGRES_PASS=password
POSTGRES_ADDR=$(ifconfig ipogif0  | grep 'inet addr' | cut -d ':' -f2 | awk '{print $1}')
export POSTGRES_ADDR
export POSTGRES_USER=postgres
export POSTGRES_DB=orion
POSTGRES_SCRATCH=$(pwd)
export POSTGRES_SCRATCH

export PREFECT_ORION_DATABASE_CONNECTION_URL="postgresql+asyncpg://$POSTGRES_USER:$POSTGRES_PASS@$POSTGRES_ADDR:5432/$POSTGRES_DB"

PREFECT_HOME="$(pwd)/prefect"
export PREFECT_HOME


if [[ $START_POSTGRES -eq 0 ]]
then
    # Need singulaity, and to remove the badness of pawsey
    echo "Loading the singularity module "
    module load singularity
    SINGULARITY_BINDPATH="$POSTGRES_SCRATCH"
    
    export SINGULARITY_BINDPATH


    if [[ ! -e "${POSTGRES_SCRATCH}/pgdata" ]]; then
        echo "Creating pgdata for the postgres server operation"
        mkdir pgdata
    fi

    if [[ ! -e postgres_latest.sif ]]
    then
        echo "Downloading the latest postgres docker container"
        singularity pull docker://postgres  
    fi
    
    SINGULARITYENV_POSTGRES_PASSWORD="$POSTGRES_PASS" SINGULARITYENV_POSTGRES_DB="$POSTGRES_DB" SINGULARITYENV_PGDATA="$POSTGRES_SCRATCH/pgdata" \
        singularity run --cleanenv --bind "$POSTGRES_SCRATCH":/var postgres_latest.sif 
fi
