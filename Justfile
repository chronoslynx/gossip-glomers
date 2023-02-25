defnodes := "3"
deflatency := "100"

build prog:
    go build -o {{prog}} {{prog}}.go

maelstrom workload prog nodes=defnodes latency=deflatency: (build prog)
    cd ../maelstrom && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}}

nemesis workload prog nemesis="partition" nodes=defnodes latency=deflatency: (build prog)
    cd ../maelstrom && ./maelstrom test -w {{workload}} --bin {{`pwd` / prog}} --node-count {{nodes}} --time-limit 20 --rate 100 --latency {{latency}} --nemesis {{nemesis}}

broadcast: (maelstrom "broadcast" "broadcast" "25" "100")
broadcast-nemesis: (nemesis "broadcast" "broadcast" "partition" "25" "100")
counter: (nemesis "g-counter" "counter" "partition" "3" "0")
unique-ids: (maelstrom "broadcast" "unique-ids" "5" "10")

debug:
    cd ../maelstrom && ./maelstrom serve
