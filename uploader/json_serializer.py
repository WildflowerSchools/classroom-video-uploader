from datetime import date, datetime


def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type %{type(obj)} not serializable")
