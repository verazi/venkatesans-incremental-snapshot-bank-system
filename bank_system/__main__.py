
from signal import SIG_DFL, SIGINT, signal
from bank_system.config import Config
from bank_system.process import GlobalSnapshot, Process
from bank_system.process_address import ProcessAddress


if __name__ == "__main__":
    import sys

    # Magic line to make ctrl+c work
    signal(SIGINT, SIG_DFL)

    if len(sys.argv) < 4:
        print("Invalid number of arguments. python -m bank_system <config_file> <process address> <process port> [,<snapshot_file>]")
        sys.exit(1)

    config_filename = sys.argv[1]
    address = sys.argv[2]
    port = int(sys.argv[3])

    if len(sys.argv) > 4:
        snapshot_file = sys.argv[4]
        with open(snapshot_file, "r") as f:
            snapshot = GlobalSnapshot.deserialise(f.read())
    else:
        snapshot = None

    with open(config_filename, "r") as f:
        config = Config.deserialise(f.read())

    process_address = ProcessAddress(address, port)

    process = Process(config, process_address)

    process.start()