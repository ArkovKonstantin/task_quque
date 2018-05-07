import socket
import collections
import pickle
from datetime import datetime


class Task:
    def __init__(self, task_id, length, data, status='handle', time=datetime.now()):
        self.task_id = task_id
        self.length = length
        self.data = data
        self.status = status
        self.time = time

    @staticmethod
    def check_time(queue_dict):
        dt = 300  # 5 минут в секундах
        for queue in queue_dict.values():
            for task in queue:
                if task.status == 'perform':
                    if (datetime.now() - task.time).seconds >= dt:
                        queue.remove(task)
                        task.status = 'handle'
                        queue.append(task)
        return queue_dict

    @staticmethod
    def write(file_name, data):
        with open(file_name, 'wb') as f:
            pickle.dump(data, f)

    @staticmethod
    def read(file_name):
        with open(file_name, 'rb') as f:
            new_data = pickle.load(f)
        return new_data


class Server:
    count_task = 0

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('127.0.0.1', 5555))  # устанавливаем host и port
        self.sock.listen(1)  # устанавливаем режим прослушивания
        self.dict_command = {'ADD': self._add, 'IN': self._in, 'ACK': self._ack, 'GET': self._get}
        self.queue_dict = collections.defaultdict(list)
        self.sock.setblocking(0)

    def run(self):
        try:
            while True:
                try:
                    conn, addr = self.sock.accept()
                except socket.error:
                    continue
                else:
                    data = conn.recv(1000000)
                    arguments = data.decode('utf-8').split(' ')
                    command = arguments.pop(0)
                    try:
                        self.queue_dict.update(Task.read('data'))  # Чтение из файла
                        self.queue_dict = Task.check_time(self.queue_dict)
                        Task.write('data', self.queue_dict)  # Запись в Файл
                    except EOFError:
                        pass
                    conn.send(self.dict_command[command](*arguments))
                    conn.close()

        except KeyboardInterrupt:
            return None

    def _add(self, queue, length, data):
        try:  # Чтение счетчика ID из файла
            self.count_task = Task.read('count')
            self.count_task += 1
        except EOFError:
            self.count_task += 1

        Task.write('count', self.count_task)  # Запись счетсика ID в Файл
        task = Task(str(self.count_task), length, data)
        self.queue_dict[queue].append(task)
        Task.write('data', self.queue_dict)  # Запись в Файл

        return bytes(str(self.queue_dict[queue][-1].task_id), 'utf-8')

    def _in(self, queue, task_id):
        for task in self.queue_dict[queue]:
            if task.task_id == task_id:
                return b'YES'
        return b'NO'

    def _get(self, queue):
        if self.queue_dict.get(queue):

            for task in self.queue_dict[queue]:
                if task.status == 'handle':
                    task.status = 'perform'
                    task.time = datetime.now()
                    print('task', task)
                    Task.write('data', self.queue_dict)  # Запись в Файл

                    return bytes('{} {} {}'.format(task.task_id, task.length, task.data), 'utf-8')
            else:
                return b'NONE'  # Вернет None если все задания исполняются

        else:
            return b'NONE'

    def _ack(self, queue, task_id):
        for task in self.queue_dict[queue]:
            if task.task_id == task_id:
                self.queue_dict[queue].remove(task)
                Task.write('data', self.queue_dict)  # Запись в Файл
                return b'YES'
        return b'NO'


if __name__ == '__main__':
    serv = Server()
    serv.run()
