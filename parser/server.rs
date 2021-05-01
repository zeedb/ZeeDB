use std::process::Command;

const SCRIPT: &str = r"
set -e

if [[ `docker ps --filter name=test-zetasql-server -q` ]]; then 
    exit 0
fi

stopped=`docker ps --all --filter name=test-zetasql-server -q`

if [[ $stopped ]]; then 
    echo 'Restarting ZetaSQL server...' >&2
    docker restart $stopped
else 
    echo 'Starting ZetaSQL server...' >&2
    docker run \
        --publish 127.0.0.1:50051:50051 \
        --name test-zetasql-server \
        --detach \
        gcr.io/analog-delight-604/zetasql-server@sha256:62325de833178537b23ae83c8bb8dce6da51112491f3e7e9f101a262533b9df7
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
