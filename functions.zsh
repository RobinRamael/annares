
function _build() {
    cargo --quiet rustc --bin node -- -Awarnings
    cargo --quiet rustc --bin client -- -Awarnings
}

function runfirst() {
    kitty cargo --quiet run --bin node $1 &
}

function run() {
    kitty cargo --quiet run --bin node $1 --peer "[::1]:$2" &
}
