
function _build() {
    cargo --quiet rustc --bin node -- -Awarnings
    cargo --quiet rustc --bin client -- -Awarnings
}

function runfirst() {
    kitty target/debug/node $1 &
}

function run() {
    kitty target/debug/node $1 --peer "[::1]:$2" &
}
