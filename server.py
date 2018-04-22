import socket
import collections
import pickle
from datetime import datetime


class Server:
    count_task = 0
    dt = '300'  # 5 минут в секундах

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('127.0.0.1', 5555))  # устанавливаем host и port
        self.sock.listen(1)  # устанавливаем режим прослушивания
        self.queue_dict = collections.defaultdict(list)

    def run(self):
        while True:
            conn, addr = self.sock.accept()
            data = conn.recv(1000000)
            arguments = data.decode('utf-8').split(' ')
            command = arguments.pop(0)
            try:
                self.queue_dict.update(self.read('data'))  # Чтение из файла
            except EOFError:
                pass

            conn.send(eval('self.' + command)(*arguments))
            conn.close()

    def ADD(self, queue, length, data):
        try:  # Чтение счетчика ID из файла
            self.count_task = self.read('count')
            self.count_task += 1
        except EOFError:
            self.count_task += 1

        self.write('count', self.count_task)  # Запись счетсика ID в Файл

        task = {'id': str(self.count_task), 'length': length, 'data': data, 'status': 'handle'}

        self.queue_dict[queue].append(task)
        self.write('data', self.queue_dict)  # Запись в Файл

        return bytes(str(self.queue_dict[queue][-1]['id']), 'utf-8')

    def IN(self, queue, task_id):
        for task in self.queue_dict[queue]:
            if task['id'] == task_id:
                return b'YES'
        return b'NO'

    def GET(self, queue):
        if self.queue_dict.get(queue):
            for task in self.queue_dict[queue]:
                if task['status'] == 'perform':
                    if str(task['time'] - datetime.now()) >= self.dt:
                        self.queue_dict[queue].remove(task)
                        task['status'] = 'handle'
                        self.queue_dict[queue].append(task)

                if task['status'] == 'handle':
                    task['status'] = 'perform'
                    task['time'] = datetime.now()
                    self.write('data', self.queue_dict)  # Запись в Файл
                    return bytes(' '.join(list(task.values())[:len(task.values())-2]), 'utf-8')

        else:
            return b'NONE'

    def ACK(self, queue, task_id):
        for task in self.queue_dict[queue]:
            if task['id'] == task_id:
                self.queue_dict[queue].remove(task)
                self.write('data', self.queue_dict)  # Запись в Файл
                return b'YES'
        return b'NO'

    def read(self, file_name):
        with open(file_name, 'rb') as f:
            new_data = pickle.load(f)
        return new_data

    def write(self, file_name, data):
        with open(file_name, 'wb') as f:
            pickle.dump(data, f)


if __name__ == '__main__':
    serv = Server()
    serv.run()
