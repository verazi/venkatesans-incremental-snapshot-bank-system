from threading import Thread
from time import sleep
from signal import signal, SIGINT, SIG_DFL

from bank_system.config import Config, ProcessConfig
from bank_system.process import Process
from bank_system.process_address import ProcessAddress

# Magic line to make ctrl+c work
signal(SIGINT, SIG_DFL)

PA = ProcessAddress

processes = {
    PA('localhost', 10101): ProcessConfig(
        PA('localhost', 10101),
        True,
        [
            PA('localhost', 10102),PA('localhost', 10103),
        ],
        [
            PA('localhost', 10102),PA('localhost', 10103),
        ],
        None,
        100,
    ),
    PA('localhost', 10102): ProcessConfig(
        PA('localhost', 10102),
        False,
        [PA('localhost', 10103),PA('localhost', 10101),],
        [],
        PA('localhost', 10101),
        200,
    ),
    PA('localhost', 10103): ProcessConfig(
        PA('localhost', 10103),
        False,
        [PA('localhost', 10101),PA('localhost', 10102),PA('localhost', 10104),],
        [PA('localhost', 10104),],
        PA('localhost', 10101),
        300,
    ),
    PA('localhost', 10104): ProcessConfig(
        PA('localhost', 10104),
        False,
        [PA('localhost', 10103),],
        [],
        PA('localhost', 10103),
        500,
    )
}

config = Config(processes)

p1 = Process(config, PA('localhost', 10101))
p2 = Process(config, PA('localhost', 10102))
p3 = Process(config, PA('localhost', 10103))
p4 = Process(config, PA('localhost', 10104))

t1 = Thread(target=p1.start)
t2 = Thread(target=p2.start)
t3 = Thread(target=p3.start)
t4 = Thread(target=p4.start)

t2.start()
sleep(1)
t3.start()
sleep(1)
t1.start()
sleep(1)
t4.start()

t3.join()
t2.join()
t1.join()
t4.join()
