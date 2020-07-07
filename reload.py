import os
import requests
import progressbar
import sqlalchemy
from sqlalchemy import create_engine, MetaData, or_, Table
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


DATA_TABLENAME = "datasets"


def requests_retry_session(
    retries=10,
    backoff_factor=0.3,
    status_forcelist=(),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def download_file(download_url, file_destination):
    with requests_retry_session(retries=3).get(url=download_url, timeout=5, stream=True) as r:
        r.raise_for_status()
        with open(file_destination, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return file_destination


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(dir_path, "data")
    if not os.path.isdir(data_path):
        os.mkdir(data_path)
    engine = create_engine('sqlite:///iati.db')
    conn = engine.connect()
    meta = MetaData(engine)
    meta.reflect()

    try:
        datasets = Table(DATA_TABLENAME, meta, autoload=True)
    except sqlalchemy.exc.NoSuchTableError:
        raise ValueError("No database found. Try running `refresh.py` first.")

    new_datasets = conn.execute(datasets.select().where(or_(datasets.c.new == 1, datasets.c.modified == 1))).fetchall()
    bar = progressbar.ProgressBar()
    print("Downloading {} new or modified datasets...".format(len(new_datasets)))
    download_errors = 0
    for dataset in bar(new_datasets):
        file_destination = os.path.join(data_path, dataset["id"])
        try:
            download_file(dataset["url"], file_destination)
            conn.execute(datasets.update().where(datasets.c.id == dataset["id"]).values(new=0, modified=0, stale=0))
        except requests.exceptions.ConnectionError:
            download_errors += 1
            continue
    print("Failed to download {} new or modified datasets.".format(download_errors))

    stale_datasets = conn.execute(datasets.select().where(datasets.c.stale == 1)).fetchall()
    print("Deleting stale {} stale datasets...".format(len(stale_datasets)))
    for dataset in stale_datasets:
        file_destination = os.path.join(data_path, dataset["id"])
        os.remove(file_destination)
        conn.execute(datasets.delete().where(datasets.c.id == dataset["id"]))

    engine.dispose()


if __name__ == '__main__':
    main()
