from dataclasses import dataclass, asdict
import datetime
from typing import Optional, Dict, Any
import json




def to_json(self):
        asdict(self)

@dataclass
class Message:
    content_type: str
    update_id: int
    user_id: str
    ts_tg: datetime.datetime
    ts_bot: datetime.datetime
    id: Optional[int] = None
    text: Optional[str] = None
    voice_link: Optional[str] = None
    class_id: Optional[str] = None

    def to_dict(self, exclude_none: bool = True) -> Dict[str, Any]:
        """Преобразует объект в словарь"""
        data = asdict(self)
        if exclude_none:
            data = {field: value for field, value in data.items() if value is not None}
        return data

    @staticmethod
    def datetime_handler(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def to_json(self):
        return json.dumps(self.to_dict(), default=Message.datetime_handler)

    @staticmethod
    def from_json(json_str):
        if json_str:
            data = json.loads(json_str)
            return Message(**data)



m = Message(content_type="text", update_id=1, user_id="1", ts_tg=datetime.datetime.now(), ts_bot=datetime.datetime.now(), text="Hello")
s= m.to_json()

m2 = Message.from_json(s)
print(m2)