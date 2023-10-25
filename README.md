# zoobench

## Usage

```bash
Usage: zoobench [OPTIONS] <HOSTS>

Arguments:
  <HOSTS>  ZooKeeper hosts

Options:
  -t, --timeout <TIMEOUT>      Connection timeout [default: 10]
  -n, --iteration <ITERATION>  Number of total znodes [default: 1000]
  -j, --threads <THREADS>      Number of threads [default: 8]
  -s, --node-size <NODE_SIZE>  ZNode value size in bytes [default: 128K]
  -e, --ephemeral              Create ephemeral znode or not
  -p, --prefix <PREFIX>        Test prefix [default: /zoobench]
  -h, --help                   Print help
  -V, --version                Print version
```

## Licence

MIT
