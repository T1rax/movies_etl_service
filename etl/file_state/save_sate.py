import abc
from typing import Any, Optional
import json


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        
    def save_state(self, state: dict):
        try:
            with open(self.file_path, 'r+') as openfile:
                json_object = json.load(openfile)
        except Exception: 
            json_object = dict()
        
        json_object.update(state)
        
        with open(self.file_path, "w+") as outfile:
            json.dump(json_object, outfile)
    
    def retrieve_state(self):
        with open(self.file_path, 'r+') as openfile:
            try: 
                json_object = json.load(openfile)
            except Exception: 
                json_object = dict()
        
        return json_object    

class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state = dict()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()

        if key in state:
            return state[key]
        else:
            return None