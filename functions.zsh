export RUST_LOG="info,peering=debug,crate::peering=debug,peering::mod=debug"

CHECK_INTERVAL=2

# omg what a mess

function build() { 
	echo cargo build
	cargo build
}

function killnodes() {
	echo killing all nodes
	pgrep annares | xargs -L1 kill
	rm -f .currently_running
}


function run_first() {
	port=$1
	redundancy=$2

	echo annares-node $port --redundancy $redundancy
	annares-node $port --redundancy $redundancy &
	echo $port >>.currently_running
}

function run() {
	port=$1
	peer=$2
	redundancy=$3

	echo annares-node $port --peer "[::1]:$peer" --redundancy $redundancy
	annares-node $port --peer "[::1]:$peer" --redundancy $redundancy &
}

function runs {
	runs=$1
	redundancy=$2

	killnodes
	clean_logs

	run_first 5001 $redundancy
	(( runs_left =  $runs - 1  ))
	run_peers $runs_left $redundancy 5002
}

function run_peers {
	runs=$1
	redundancy=$2
	starting_port=$3

	((last_port = $starting_port + $runs - 1))
	for port in {$starting_port..$last_port}; do
		sleep 1
		((peer = $port - 1))
		run $port $peer $redundancy 
		echo $port >> .currently_running
	done
}


function enter_random_words {
	echo adding $1 words
	for value in $(shuf -n $1 words); do
		node=$(shuf -n 1 .currently_running)
		echo "$value from $node"
		client store $value -p ":$node" | grep under | cut -f2 -d' ' >>.added_keys
	done
}

function enter_words {
	start=$1
	n=$2

	echo adding $n words starting from $seed

	((end = $1 + $2 - 1 ))
	for value in $(cat words | sed -n "$start,$end p"); do
		node=$(shuf -n 1 .currently_running)
		echo store "$value using $node"
		client store $value -p ":$node" | grep under | cut -f2 -d' ' >>.added_keys
	done
}

function check {
	for key in $(cat .added_keys); do
		shortkey=$(echo $key | sed 's/.*\(....\)$/\1/g')
		peer=$(shuf -n 1 .currently_running)
		echo "getting $shortkey from $peer"
		client get $key -p ":$peer"
	done
}

function shutdown {
	node=$1
	client shutdown -p ":$node"
	sed -i "/$node/d" .currently_running
}

function monitor_logs() {
	kitty tail -f /tmp/logs/annares.log &
	kitty sh -c "tail -f /tmp/logs/annares.log | grep ERROR" &
}

function clean_logs() {
	pgrep -f annares.log | xargs kill
	rm -rf /tmp/logs/annares.log*
	echo "" > /tmp/logs/annares.log
}

function debugleave {
	n=$1
	redundancy=$2
	seed=$3
	n_words=$4
	kill_n=$5

	runs $n $redundancy

	echo "press enter to monitor logs"
	read
	monitor_logs


	echo "press enter to enter $4 values"
	read
	enter_words $seed $n_words

	echo "press enter to kill $kill_n random nodes"
	read
	for i in {1..$kill_n}; do
		killrandomly
	done

	echo "press enter to check"
	read
	check

	echo "press enter to close log windows"
	read
}


function debugjoin {
	initial_n=$1
	redundancy=$2
	seed=$3
	n_words=$4
	new_n=$5

	clean_logs

	runs $initial_n $redundancy

	echo "press enter to monitor logs"
	read
	monitor_logs


	echo "press enter to enter $4 values"
	read
	enter_words $seed $n_words

	echo "press enter to let $5 nodes join the network"
	read

	((starting_port = 5001 + $initial_n))
	run_peers $new_n $redundancy $starting_port

	echo "press enter to check"
	read
	check

	echo "press enter to close log windows"
	read
	pgrep -f annares.log | xargs kill

}


function shutdownrandomly {
	peer=$(shuf -n 1 .currently_running)
	echo killing $peer
	killnode $peer
}

function monitor {
	((last_port = 5000 + $1))
	for port in {5001..$last_port}; do
		echo launching monitor for $port
		kitty watch client status -p "[::1]:$port" &
	done
}
