from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
@dataclass(frozen=True)
class Record:
    index:int; actor:str; action:str; subject:str; at:datetime; previous:str; digest:str; key:str|None=None
class IdempotencyConflict(RuntimeError):
    """A keyed audit entry contradicted the record already booked for that key."""
    def __init__(self,key,original,replay):
        super().__init__(f"audit idempotency conflict for key {key}")
        self.key=key; self.original=original; self.replay=replay
class AuditTrail:
    def __init__(self): self.records=[]; self._keys={}
    @staticmethod
    def digest(index,actor,action,subject,at,previous,key=None):
        raw=json.dumps([index,actor,action,subject,at.astimezone(timezone.utc).isoformat(),previous,key],separators=(',',':')).encode()
        return hashlib.sha256(raw).hexdigest()
    def append(self,actor,action,subject,at=None,key=None):
        at=at or datetime.now(timezone.utc)
        if key is not None and key in self._keys:
            prior=self._keys[key]
            if (prior.actor,prior.action,prior.subject,prior.at)!=(actor,action,subject,at):
                raise IdempotencyConflict(key,prior,Record(0,actor,action,subject,at,'','',key))
            return prior
        prev=self.records[-1].digest if self.records else 'GENESIS'; i=len(self.records)+1
        r=Record(i,actor,action,subject,at,prev,self.digest(i,actor,action,subject,at,prev,key),key);self.records.append(r)
        if key is not None:self._keys[key]=r
        return r
    def keyed(self,key):
        return self._keys.get(key)
    def stats(self):
        out={}
        for r in self.records: out[r.action]=out.get(r.action,0)+1
        return out
    def verify(self):
        prev='GENESIS'
        seen=set()
        for r in self.records:
            if r.previous!=prev or r.digest!=self.digest(r.index,r.actor,r.action,r.subject,r.at,prev,r.key): return False
            if r.key is not None:
                if r.key in seen: return False
                seen.add(r.key)
            prev=r.digest
        return True
