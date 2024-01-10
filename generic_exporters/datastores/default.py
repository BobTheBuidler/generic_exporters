
import errno
from os import mkdir, path


def _ensure_default_storage_path_exists() -> None:
    try:
        mkdir(f"{path.expanduser( '~' )}/.generic_exporters")
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

_ensure_default_storage_path_exists()

sqlite_settings = {
    'provider': "sqlite",
    'filename': f"{path.expanduser( '~' )}/.generic_exporters/generic_exporters.sqlite",
    'create_db': True,
}
