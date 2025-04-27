from datetime import datetime

def strtotime(date_str):
    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M')