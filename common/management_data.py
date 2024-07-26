from abc import ABC, abstractmethod
import os


class BaseManagementData(ABC):
    def __init__(self, base_path, file_name, os, pd):
        self.base_path = base_path
        self.file_name = file_name
        self.os = os
        self.pd = pd

    @abstractmethod
    def ensure_dir_exist(self):
        raise NotImplementedError

    @abstractmethod
    def save_data(self):
        raise NotImplementedError

    @abstractmethod
    def load_data(self):
        raise NotImplementedError


class BaseManagementCSVData(BaseManagementData):
    def __init__(self, base_path, file_name, os, pd):
        super().__init__(base_path, file_name, os, pd)
        self.base_path = base_path
        self.file_name = file_name
        self.os = os
        self.pd = pd

    def ensure_dir_exist(self, path):
        if not self.os.path.exists(path):
            self.os.makedirs(path)

    def clean_data(self, data):
        cleaned_data = []
        for row in data:
            cleaned_row = {k: v.replace('\n', ' ').strip() if isinstance(
                v, str) else v for k, v in row.items()}
            cleaned_data.append(cleaned_row)
        return cleaned_data

    def save_data(self, data):
        file_path = self.os.path.join(self.base_path, self.file_name)
        print(f'file{file_path}')
        directory = os.path.dirname(file_path)
        print(f'directory{directory}')
        self.ensure_dir_exist(directory)
        # cleaned_data = self.clean_data(data)
        df = self.pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        print(f'Data saved to {file_path}')

    def load_data(self):
        file_path = self.os.path.join(self.base_path, self.file_name)
        if not self.os.path.exists(file_path):
            raise FileNotFoundError(f'{file_path} does not exist.')
        df = self.pd.read_csv(file_path)
        return df.to_dict(orient='records')
