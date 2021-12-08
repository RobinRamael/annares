export RUST_LOG="info,peering=debug,crate::peering=debug,peering::mod=debug"

CHECK_INTERVAL=2

# omg what a mess

function runfirst() {

	echo cargo build
	cargo build
	echo annares-node $1 --redundancy $2
	annares-node $1 --redundancy $2 &
}

function run() {
	echo annares-node $1 --peer "[::1]:$2" --redundancy $3
	annares-node $1 --peer "[::1]:$2" --redundancy $3 &
}

function runs {
	echo killing all nodes
	pgrep -f annares-node | xargs kill
	rm .currently_running
	rm .added_keys
	runfirst 5001 $2
	echo 5001
	echo 5001 >>.currently_running
	((last_port = 5000 + $1 - 1))
	for peer in {5001..$last_port}; do
		sleep 1
		((port = $peer + 1))
		echo $port 5001
		run $port 5001 $2 &
		echo $port >>.currently_running
	done
}

function enter_data {
	for value in fox bear raccoon doggo bertha; do
		client store $value -p :5001
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
	echo adding $2 words starting from $1
	((n = $1 + $2))
	for value in $(cat words | sed -n "$1,$n p"); do
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

function debug {
	pgrep -f annares.log | xargs kill
	rm -rf /tmp/logs/annares.log*
	echo "" > /tmp/logs/annares.log
	runs $1 $2

	kitty tail -f /tmp/logs/annares.log &
	kitty sh -c "tail -f /tmp/logs/annares.log | grep ERROR" &


	echo "press enter to enter $4 values"
	read
	enter_words $3 $4

	echo "press enter to kill $5 random nodes"
	read
	for i in {1..$5}; do
		killrandomly
	done

	echo "press enter to check"
	read
	check

	echo "press enter to close log windows"
	read
	pgrep -f annares.log | xargs kill

}

function killrandomly {
	peer=$(shuf -n 1 .currently_running)
	echo killing $peer
	killnode $peer
}
