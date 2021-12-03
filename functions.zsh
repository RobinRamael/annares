
CHECK_INTERVAL=2


function runfirst() {

    echo cargo build
    cargo build
    echo target/debug/node $1 --check-interval $CHECK_INTERVAL &
    target/debug/node $1 --check-interval $CHECK_INTERVAL &
}

function run() {
    echo target/debug/node $1 --peer "[::1]:$2" --check-interval $CHECK_INTERVAL
    target/debug/node $1 --peer "[::1]:$2"  --check-interval $CHECK_INTERVAL &
}

function runs {
    echo killing all nodes
    pkill node
    runfirst 5001;
    echo 5001
    ((last_port = 5000 + $1 - 1))
    for peer in {5001..$last_port}; do
        ((port = $peer + 1));
        sleep 1;
        echo $port $peer
        run $port $peer &;
    done
}

function enter_data {
    for value in fox bear raccoon doggo bertha
    do
        client store $value -p :5001
    done
}

function enter_words {
    for value in $(cat words)
    do
        client store $value -p :5001
    done
}

function monitor {
    ((last_port = 5000 + $1))
    for port in {5001..$last_port}; do
        echo launching monitor for $port
        kitty watch client get-all -p "[::1]:$port" &
    done
}
