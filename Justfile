defnodes := "3"
deflatency := "100"

build prog:
    cd {{prog}} && go build -o aut main.go

maelstrom workload prog nodes=defnodes latency=deflatency: (build prog)
    cd ../maelstrom/maelstrom && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog / "aut"}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}}

nemesis workload prog nemesis="partition" nodes=defnodes latency=deflatency: (build prog)
    cd ../maelstrom/maelstrom && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog / "aut"}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}} --nemesis {{nemesis}}

broadcast: (maelstrom "broadcast" "broadcast" "25" "100")
broadcast-nemesis: (nemesis "broadcast" "broadcast" "partition" "25" "100")
counter: (nemesis "g-counter" "counter" "partition" "3" "0")
counter-crdt: (nemesis "g-counter" "counter-crdt" "partition" "3" "0")
unique-ids: (maelstrom "broadcast" "unique-ids" "5" "10")

debug:
    cd ../maelstrom && ./maelstrom serve
