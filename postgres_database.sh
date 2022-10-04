# Need singulaity, and to remove the badness of pawsey
module load singularity
unset SINGULARITY_BINDPATH

# Now set up some postgres values
export POSTGRES_PASS=password
export POSTGRES_ADDR=127.0.0.1
export POSTGRES_USER=postgres
export POSTGRES_DB=orion
export POSTGRES_SCRATCH=$(pwd)

if [[ ! -e "${POSTGRES_SCRATCH}/pgdata" ]]; then
    echo "Creating pgdata"
    mkdir pgdata
fi

singularity pull docker://postgres  ## singularity pull has to be done only once
SINGULARITYENV_POSTGRES_PASSWORD=$POSTGRES_PASS SINGULARITYENV_POSTGRES_DB=$POSTGRES_DB SINGULARITYENV_PGDATA=$POSTGRES_SCRATCH/pgdata \
    singularity run --cleanenv --bind $POSTGRES_SCRATCH:/var postgres_latest.sif &

export PREFECT_ORION_DATABASE_CONNECTION_URL="postgresql+asyncpg://$POSTGRES_USER:$POSTGRES_PASS@$POSTGRES_ADDR:5432/$POSTGRES_DB"


#export PREFECT_ORION_DATABASE_CONNECTION_URL="sqlite+aiosqlite:////$(pwd)/test_db.db"

export PREFECT_HOME="$(pwd)/prefect"
