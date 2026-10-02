from dataclasses import dataclass
from threading import RLock
@dataclass(frozen=True)
class Event:
    stream:str; sequence:int; kind:str; payload:dict; key:str|None=None
class IdempotencyConflict(RuntimeError):
    """A keyed append contradicted the event already booked for that key."""
    def __init__(self, stream, key, original, replay):
        super().__init__(f"idempotency conflict on {stream} key {key}")
        self.stream=stream; self.key=key; self.original=original; self.replay=replay
class EventStore:
    def __init__(self): self._data={}; self._keys={}; self._lock=RLock()
    def append(self,stream,expected,kind,payload,key=None):
        with self._lock:
            rows=self._data.setdefault(stream,[])
            if len(rows)!=expected: raise RuntimeError('stream conflict')
            keys=self._keys.setdefault(stream,{})
            if key is not None and key in keys:
                prior=keys[key]
                if (prior.kind,prior.payload)!=(kind,dict(payload)):
                    raise IdempotencyConflict(stream,key,prior,Event(stream,0,kind,dict(payload),key))
                return prior
            e=Event(stream,len(rows)+1,kind,dict(payload),key);rows.append(e)
            if key is not None: keys[key]=e
            return e
    def read(self,stream,after=0):
        with self._lock:return [e for e in self._data.get(stream,[]) if e.sequence>after]
    def keyed(self,stream,key):
        with self._lock:return self._keys.get(stream,{}).get(key)
    def head(self,stream):
        with self._lock:return len(self._data.get(stream,[]))
    def all_streams(self):
        with self._lock:return sorted(self._data)
