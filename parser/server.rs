use std::process::Command;

const SCRIPT: &str = r"
set -eu

if [[ `docker ps --filter name=zetasql -q` ]]; then 
    exit 0
fi

stopped=`docker ps --all --filter name=zetasql -q`

if [[ $stopped ]]; then 
    echo 'Restarting ZetaSQL server...' >&2
    docker restart $stopped
else 
    echo 'Starting ZetaSQL server...' >&2
    docker run \
        --publish 127.0.0.1:50051:50051 \
        --name zetasql \
        --detach \
        gcr.io/zeedeebee/zetasql
fi

until nc -z 127.0.0.1 50051
do

    echo '...waiting for ZetaSQL server...' >&2
    sleep 0.5
done
echo '...ZetaSQL server has started' >&2
";

/// Run the startup script for the local ZetaSQL server process.
pub fn start_server_process() {
    Command::new("sh")
        .arg("-c")
        .arg(SCRIPT)
        .status()
        .expect("failed to start docker");
}
