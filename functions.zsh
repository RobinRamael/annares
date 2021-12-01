

function runfirst() {
    cargo build
    kitty target/debug/node $1 &
}

function run() {
    kitty target/debug/node $1 --peer "[::1]:$2" &
}
