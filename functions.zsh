
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
    rm .currently_running
    rm .added_keys
    runfirst 5001;
    echo 5001
    echo 5001 >> .currently_running
    ((last_port = 5000 + $1 - 1))
    for peer in {5001..$last_port}; do
        ((port = $peer + 1));
        sleep 1;
        echo $port $peer
        run $port $peer &;
        echo $port >> .currently_running
    done
}

function enter_data {
    for value in fox bear raccoon doggo bertha
    do
        client store $value -p :5001
    done
}

function enter_words {
    echo adding $1 words
    for value in $(shuf -n $1 words);
    do
        echo $value
        client store $value -p ":$(shuf -n 1 .currently_running)" | grep under | cut -f2 -d' '  >> .added_keys
    done
}

function check {
    for value in $(cat .added_keys)
    do
        client get $value -p ":$(shuf -n 1 .currently_running)"
    done
}

function killnode {
    sed -i "/$1/d" .currently_running 
    client shutdown -p ":$1"
}

function monitor {
    ((last_port = 5000 + $1))
    for port in {5001..$last_port}; do
        echo launching monitor for $port
        kitty watch client status -p "[::1]:$port" &
    done
}
