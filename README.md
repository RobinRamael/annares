
# Annares

A toy distributed hash table written in Rust to teach myself the language.


## Usage

```
cargo build

# run some nodes

target/debug/node 5001                  # running on port 5001
target/debug/node 5002 -p "[::]:5001"   # use the given address to join the network
target/debug/node 5003 -p "[::]:5001"


# store a value in the network by passing it to a node with the client:
target/debug/client store thisisavaluethatwillbestored -p "[::]:5001"
# > Stored in [::1]:5001
# > under 4f22b3b058ac84e152c9841b1cd551d75416b0d600971b2b375ab35755cd20d4


target/debug/client get 4f22b3b058ac84e152c9841b1cd551d75416b0d600971b2b375ab35755cd20d4 -p "[::]:5003"
# > Value thisisavaluethatwillbestored
# > was in [::1]:5001

```

The zsh functions in `functions.zsh` can help with setting up more nodes and entering lots of data to test.
