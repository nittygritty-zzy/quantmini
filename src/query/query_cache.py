# Query cache implementation
import hashlib
import pickle
from pathlib import Path
from typing import Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class QueryCache:
    def __init__(self, cache_root: Path, max_size_gb: float = 2.0):
        self.cache_root = Path(cache_root)
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.max_size_bytes = int(max_size_gb * 1024**3)
        self.metadata_file = self.cache_root / 'metadata.pkl'
        self._load_metadata()

    def _load_metadata(self):
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'rb') as f:
                    self.metadata = pickle.load(f)
            except:
                self._init_metadata()
        else:
            self._init_metadata()

    def _init_metadata(self):
        self.metadata = {'entries': {}, 'hits': 0, 'misses': 0, 'total_size': 0}

    def _save_metadata(self):
        with open(self.metadata_file, 'wb') as f:
            pickle.dump(self.metadata, f)

    def make_key(self, **kwargs) -> str:
        sorted_items = sorted(kwargs.items())
        key_str = str(sorted_items)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get(self, key: str) -> Optional[pd.DataFrame]:
        if key not in self.metadata['entries']:
            self.metadata['misses'] += 1
            return None
        entry = self.metadata['entries'][key]
        cache_file = self.cache_root / entry['file']
        if not cache_file.exists():
            del self.metadata['entries'][key]
            self.metadata['misses'] += 1
            return None
        try:
            with open(cache_file, 'rb') as f:
                result = pickle.load(f)
            entry['last_used'] = pd.Timestamp.now()
            self.metadata['hits'] += 1
            self._save_metadata()
            return result
        except:
            del self.metadata['entries'][key]
            self.metadata['misses'] += 1
            return None

    def put(self, key: str, data: pd.DataFrame):
        try:
            cache_file = self.cache_root / f'{key}.pkl'
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            file_size = cache_file.stat().st_size
            if key in self.metadata['entries']:
                old_size = self.metadata['entries'][key]['size']
                self.metadata['total_size'] -= old_size
            self.metadata['entries'][key] = {
                'file': f'{key}.pkl',
                'size': file_size,
                'last_used': pd.Timestamp.now()
            }
            self.metadata['total_size'] += file_size
            self._evict_if_needed()
            self._save_metadata()
        except Exception as e:
            logger.error(f"Failed to cache: {e}")

    def _evict_if_needed(self):
        while self.metadata['total_size'] > self.max_size_bytes and self.metadata['entries']:
            lru_key = min(self.metadata['entries'].keys(), 
                         key=lambda k: self.metadata['entries'][k]['last_used'])
            entry = self.metadata['entries'].pop(lru_key)
            cache_file = self.cache_root / entry['file']
            if cache_file.exists():
                cache_file.unlink()
            self.metadata['total_size'] -= entry['size']

    def get_stats(self) -> dict:
        total_requests = self.metadata['hits'] + self.metadata['misses']
        hit_rate = self.metadata['hits'] / total_requests if total_requests > 0 else 0
        return {
            'hits': self.metadata['hits'],
            'misses': self.metadata['misses'],
            'hit_rate': hit_rate,
            'entries': len(self.metadata['entries']),
            'total_size_mb': self.metadata['total_size'] / (1024**2)
        }

    def clear(self):
        for entry in self.metadata['entries'].values():
            cache_file = self.cache_root / entry['file']
            if cache_file.exists():
                cache_file.unlink()
        self._init_metadata()
        self._save_metadata()

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (f"QueryCache(entries={stats['entries']}, "
                f"size={stats['total_size_mb']:.1f}MB, "
                f"hit_rate={stats['hit_rate']:.1%})")
