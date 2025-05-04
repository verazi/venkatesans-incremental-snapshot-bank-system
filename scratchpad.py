from threading import Thread
from signal import signal, SIGINT, SIG_DFL

from bank_system.config import ProcessAddress, Config, ProcessConfig
from bank_system.process import Process

# Magic line to make ctrl+c work
signal(SIGINT, SIG_DFL)

processes = {
    ProcessAddress('localhost', 10101): ProcessConfig(
        ProcessAddress('localhost', 10101),
        True,
        [ProcessAddress('localhost', 10102),],
        100,
        []
    ),
    ProcessAddress('localhost', 10102): ProcessConfig(
        ProcessAddress('localhost', 10102),
        True,
        [ProcessAddress('localhost', 10101),],
        200,
        []
    )
}

config = Config(processes)

p1 = Process(config, ProcessAddress('localhost', 10101))
p2 = Process(config, ProcessAddress('localhost', 10102))

t1 = Thread(target=p1.start)
t2 = Thread(target=p2.start)

t1.start()
t2.start()

t1.join()
t2.join()
