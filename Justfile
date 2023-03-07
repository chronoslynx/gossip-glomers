defnodes := "3"
deflatency := "100"

# asseming self-build maelstrom
maelstrom_dir := env_var_or_default("MAELSTROM", "../maelstrom/maelstrom")

build prog:
    cd {{prog}} && go build -o aut main.go

maelstrom workload prog nodes=defnodes latency=deflatency: (build prog)
    cd {{maelstrom_dir}} && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog / "aut"}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}}

nemesis workload prog nemesis="partition" nodes=defnodes latency=deflatency: (build prog)
    cd {{maelstrom_dir}} && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog / "aut"}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}} --nemesis {{nemesis}}

broadcast: (maelstrom "broadcast" "broadcast" "25" "100")
broadcast-nemesis: (nemesis "broadcast" "broadcast" "partition" "25" "100")
counter: (nemesis "g-counter" "counter" "partition" "3" "0")
counter-crdt: (nemesis "g-counter" "counter-crdt" "partition" "3" "0")
unique-ids: (maelstrom "unique-ids" "unique-ids" "5" "10")
kafka: (build "kafka")
    cd {{maelstrom_dir}} && ./maelstrom test -w kafka --bin {{`pwd` / "kafka" / "aut"}} --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

txns-a: (build "txns")
    cd {{maelstrom_dir}} && ./maelstrom test -w txn-rw-register --bin {{`pwd` / "txns" / "aut"}} --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
txns-b: (build "txns")
    cd {{maelstrom_dir}} && ./maelstrom test -w txn-rw-register --bin {{`pwd` / "txns" / "aut"}} --node-count 2 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

debug:
    cd {{maelstrom_dir}} && ./maelstrom serve
