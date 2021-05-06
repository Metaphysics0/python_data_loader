import pandas as pd
from columns import columns


def load_data(csv_path, columns):
    data = pd.read_csv(csv_path)
    df = pd.DataFrame(data, columns=columns)


if __name__ in '__main__':
    load_data('./covid-data.csv', columns)
