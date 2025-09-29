import hashlib
import os
import glob
from typing import List, Iterable


def sha256_file(path: str) -> str:
    """Хеш файла (sha256) — стабильный потоковый подсчёт."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def list_files(root: str, patterns: Iterable[str]) -> List[str]:
    """
    Возвращает список файлов, соответствующих списку шаблонов (glob).
    patterns: например ['*.xlsx', '*.csv']
    Результат детерминирован — сортируется по имени.
    """
    files = []
    for p in patterns:
        # Не принимать пустые шаблоны
        p = p.strip()
        if not p:
            continue
        files.extend(glob.glob(os.path.join(root, p)))
    files = [f for f in files if os.path.isfile(f)]
    return sorted(files)
